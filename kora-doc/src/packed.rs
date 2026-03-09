//! Packed document encoding primitives.

use thiserror::Error;

use crate::registry::FieldId;

const HEADER_LEN: usize = 8;
const OFFSET_ENTRY_LEN: usize = 4;

const TYPE_NULL: u8 = 0x00;
const TYPE_BOOL_FALSE: u8 = 0x01;
const TYPE_BOOL_TRUE: u8 = 0x02;
const TYPE_I64: u8 = 0x03;
const TYPE_F64: u8 = 0x04;
const TYPE_STR_INLINE: u8 = 0x05;
const TYPE_STR_DICTREF: u8 = 0x06;
const TYPE_ARRAY: u8 = 0x07;

#[derive(Debug, Clone, Copy)]
struct Layout {
    field_count: usize,
    type_table_start: usize,
    data_region_start: usize,
}

#[derive(Debug, Clone, Copy)]
struct OffsetEntry {
    field_id: FieldId,
    data_offset: u16,
}

/// Packed field value representation.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed integer value.
    I64(i64),
    /// Floating-point value.
    F64(f64),
    /// Inline bytes value.
    InlineBytes(Vec<u8>),
    /// Dictionary reference value.
    DictRef(u32),
    /// Structured JSON payload encoded as bytes (arrays and empty objects).
    ArrayBytes(Vec<u8>),
}

/// Packed document encoding/decoding errors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PackedDocError {
    /// The packed buffer is malformed.
    #[error("malformed packed document: {0}")]
    Malformed(&'static str),
    /// Builder encountered too many fields for `u16` field count.
    #[error("too many fields in packed document: {0}")]
    TooManyFields(usize),
    /// Duplicate field ID in builder input.
    #[error("duplicate field id in packed document: {0}")]
    DuplicateFieldId(FieldId),
    /// A field payload is too large for the packed format.
    #[error("field {field_id} payload too large: {len} bytes")]
    FieldDataTooLarge {
        /// Field ID that exceeded size bounds.
        field_id: FieldId,
        /// Payload length in bytes.
        len: usize,
    },
    /// Total packed data region exceeds `u16` offsets.
    #[error("data region too large: {0} bytes")]
    DataRegionTooLarge(usize),
    /// Unknown type tag in a packed document.
    #[error("unknown field type tag: 0x{0:02x}")]
    UnknownTypeTag(u8),
    /// Field payload bytes do not match type expectations.
    #[error("invalid data for field {field_id}: {reason}")]
    InvalidFieldData {
        /// Field ID with invalid payload.
        field_id: FieldId,
        /// Validation error description.
        reason: &'static str,
    },
}

/// Packed document container.
#[derive(Debug, Clone, PartialEq)]
pub struct PackedDoc {
    data: Vec<u8>,
}

impl PackedDoc {
    /// Construct a packed document from bytes and validate structural integrity.
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, PackedDocError> {
        let doc = Self { data };
        doc.validate()?;
        Ok(doc)
    }

    /// Return a borrowed view of the underlying bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consume the packed document and return owned bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    /// Return total byte size.
    #[must_use]
    pub fn byte_size(&self) -> usize {
        self.data.len()
    }

    /// Return packed format version.
    pub fn version(&self) -> Result<u16, PackedDocError> {
        let _layout = self.layout()?;
        Ok(u16::from_le_bytes([self.data[0], self.data[1]]))
    }

    /// Return number of fields in this packed document.
    pub fn field_count(&self) -> Result<usize, PackedDocError> {
        Ok(self.layout()?.field_count)
    }

    /// Return document update timestamp.
    pub fn updated_at(&self) -> Result<u32, PackedDocError> {
        let _layout = self.layout()?;
        Ok(u32::from_le_bytes([
            self.data[4],
            self.data[5],
            self.data[6],
            self.data[7],
        ]))
    }

    /// Read a field by field ID.
    pub fn read_field(&self, field_id: FieldId) -> Result<Option<FieldValue>, PackedDocError> {
        let layout = self.layout()?;

        let mut lo = 0usize;
        let mut hi = layout.field_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = self.offset_entry(mid, &layout)?;
            if entry.field_id == field_id {
                let (_, value) = self.decode_at_index(mid, &layout)?;
                return Ok(Some(value));
            }
            if entry.field_id < field_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        Ok(None)
    }

    /// Read multiple fields in the order requested.
    pub fn read_fields(
        &self,
        field_ids: &[FieldId],
    ) -> Result<Vec<Option<FieldValue>>, PackedDocError> {
        field_ids.iter().map(|id| self.read_field(*id)).collect()
    }

    /// Iterate over all packed fields in sorted field-id order.
    pub fn iter_fields(&self) -> Result<PackedDocIter<'_>, PackedDocError> {
        let layout = self.layout()?;
        Ok(PackedDocIter {
            doc: self,
            layout,
            index: 0,
        })
    }

    fn validate(&self) -> Result<(), PackedDocError> {
        let layout = self.layout()?;
        let mut previous_field_id: Option<FieldId> = None;
        let mut previous_offset: Option<u16> = None;

        for index in 0..layout.field_count {
            let entry = self.offset_entry(index, &layout)?;

            if let Some(prev_id) = previous_field_id {
                if entry.field_id <= prev_id {
                    if entry.field_id == prev_id {
                        return Err(PackedDocError::DuplicateFieldId(entry.field_id));
                    }
                    return Err(PackedDocError::Malformed(
                        "field ids must be strictly ascending",
                    ));
                }
            }

            if let Some(prev_offset) = previous_offset {
                if entry.data_offset < prev_offset {
                    return Err(PackedDocError::Malformed(
                        "field offsets must be monotonically increasing",
                    ));
                }
            }

            let _ = self.decode_at_index(index, &layout)?;
            previous_field_id = Some(entry.field_id);
            previous_offset = Some(entry.data_offset);
        }

        Ok(())
    }

    fn layout(&self) -> Result<Layout, PackedDocError> {
        if self.data.len() < HEADER_LEN {
            return Err(PackedDocError::Malformed("buffer shorter than header"));
        }

        let field_count = u16::from_le_bytes([self.data[2], self.data[3]]) as usize;

        let offset_table_bytes = field_count
            .checked_mul(OFFSET_ENTRY_LEN)
            .ok_or(PackedDocError::Malformed("offset table length overflow"))?;
        let type_table_start = HEADER_LEN
            .checked_add(offset_table_bytes)
            .ok_or(PackedDocError::Malformed("type table start overflow"))?;
        let data_region_start = type_table_start
            .checked_add(field_count)
            .ok_or(PackedDocError::Malformed("data region start overflow"))?;

        if self.data.len() < data_region_start {
            return Err(PackedDocError::Malformed(
                "buffer shorter than table region",
            ));
        }

        Ok(Layout {
            field_count,
            type_table_start,
            data_region_start,
        })
    }

    fn offset_entry(&self, index: usize, layout: &Layout) -> Result<OffsetEntry, PackedDocError> {
        if index >= layout.field_count {
            return Err(PackedDocError::Malformed(
                "offset entry index out of bounds",
            ));
        }

        let start = HEADER_LEN
            .checked_add(
                index
                    .checked_mul(OFFSET_ENTRY_LEN)
                    .ok_or(PackedDocError::Malformed("offset entry start overflow"))?,
            )
            .ok_or(PackedDocError::Malformed("offset entry start overflow"))?;
        let end = start
            .checked_add(OFFSET_ENTRY_LEN)
            .ok_or(PackedDocError::Malformed("offset entry end overflow"))?;

        let bytes = self
            .data
            .get(start..end)
            .ok_or(PackedDocError::Malformed("offset entry out of bounds"))?;

        Ok(OffsetEntry {
            field_id: u16::from_le_bytes([bytes[0], bytes[1]]),
            data_offset: u16::from_le_bytes([bytes[2], bytes[3]]),
        })
    }

    fn decode_at_index(
        &self,
        index: usize,
        layout: &Layout,
    ) -> Result<(FieldId, FieldValue), PackedDocError> {
        let entry = self.offset_entry(index, layout)?;
        let next_offset = if index + 1 < layout.field_count {
            self.offset_entry(index + 1, layout)?.data_offset as usize
        } else {
            self.data
                .len()
                .checked_sub(layout.data_region_start)
                .ok_or(PackedDocError::Malformed("data region underflow"))?
        };
        let current_offset = entry.data_offset as usize;

        if next_offset < current_offset {
            return Err(PackedDocError::Malformed("field offsets are not monotonic"));
        }

        let data_start = layout
            .data_region_start
            .checked_add(current_offset)
            .ok_or(PackedDocError::Malformed("field data start overflow"))?;
        let data_end = layout
            .data_region_start
            .checked_add(next_offset)
            .ok_or(PackedDocError::Malformed("field data end overflow"))?;

        let tag = *self
            .data
            .get(layout.type_table_start + index)
            .ok_or(PackedDocError::Malformed("type tag out of bounds"))?;
        let slice = self
            .data
            .get(data_start..data_end)
            .ok_or(PackedDocError::Malformed("field data out of bounds"))?;
        let value = decode_field_value(tag, slice, entry.field_id)?;

        Ok((entry.field_id, value))
    }
}

/// Iterator over packed fields.
pub struct PackedDocIter<'a> {
    doc: &'a PackedDoc,
    layout: Layout,
    index: usize,
}

impl<'a> Iterator for PackedDocIter<'a> {
    type Item = Result<(FieldId, FieldValue), PackedDocError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.layout.field_count {
            return None;
        }

        let result = self.doc.decode_at_index(self.index, &self.layout);
        self.index += 1;
        Some(result)
    }
}

#[derive(Debug, Clone)]
struct BuilderField {
    field_id: FieldId,
    tag: u8,
    data: Vec<u8>,
}

/// Packed document builder.
#[derive(Debug, Default)]
pub struct PackedDocBuilder {
    version: u16,
    fields: Vec<BuilderField>,
}

impl PackedDocBuilder {
    /// Create a new builder for a specific packed format version.
    #[must_use]
    pub fn new(version: u16) -> Self {
        Self {
            version,
            fields: Vec::new(),
        }
    }

    /// Add a field entry to the builder.
    pub fn add_field(
        &mut self,
        field_id: FieldId,
        value: FieldValue,
    ) -> Result<(), PackedDocError> {
        let (tag, data) = encode_field_value(field_id, value)?;
        self.fields.push(BuilderField {
            field_id,
            tag,
            data,
        });
        Ok(())
    }

    /// Build a packed document using `updated_at` seconds.
    pub fn build(mut self, updated_at: u32) -> Result<PackedDoc, PackedDocError> {
        self.fields.sort_by_key(|f| f.field_id);

        for window in self.fields.windows(2) {
            if window[0].field_id == window[1].field_id {
                return Err(PackedDocError::DuplicateFieldId(window[0].field_id));
            }
        }

        let field_count = self.fields.len();
        let field_count_u16 =
            u16::try_from(field_count).map_err(|_| PackedDocError::TooManyFields(field_count))?;
        let data_size = self.fields.iter().try_fold(0usize, |acc, field| {
            acc.checked_add(field.data.len())
                .ok_or(PackedDocError::DataRegionTooLarge(usize::MAX))
        })?;

        if data_size > u16::MAX as usize {
            return Err(PackedDocError::DataRegionTooLarge(data_size));
        }

        let offset_table_size = field_count
            .checked_mul(OFFSET_ENTRY_LEN)
            .ok_or(PackedDocError::Malformed("offset table size overflow"))?;
        let type_table_size = field_count;
        let total_size = HEADER_LEN
            .checked_add(offset_table_size)
            .and_then(|n| n.checked_add(type_table_size))
            .and_then(|n| n.checked_add(data_size))
            .ok_or(PackedDocError::Malformed("packed document size overflow"))?;

        let mut data = Vec::with_capacity(total_size);

        data.extend_from_slice(&self.version.to_le_bytes());
        data.extend_from_slice(&field_count_u16.to_le_bytes());
        data.extend_from_slice(&updated_at.to_le_bytes());

        let mut data_offset = 0u16;
        for field in &self.fields {
            data.extend_from_slice(&field.field_id.to_le_bytes());
            data.extend_from_slice(&data_offset.to_le_bytes());
            data_offset = data_offset
                .checked_add(field.data.len() as u16)
                .ok_or(PackedDocError::DataRegionTooLarge(data_size))?;
        }

        for field in &self.fields {
            data.push(field.tag);
        }

        for field in &self.fields {
            data.extend_from_slice(&field.data);
        }

        PackedDoc::from_bytes(data)
    }
}

fn encode_field_value(
    field_id: FieldId,
    value: FieldValue,
) -> Result<(u8, Vec<u8>), PackedDocError> {
    match value {
        FieldValue::Null => Ok((TYPE_NULL, Vec::new())),
        FieldValue::Bool(false) => Ok((TYPE_BOOL_FALSE, Vec::new())),
        FieldValue::Bool(true) => Ok((TYPE_BOOL_TRUE, Vec::new())),
        FieldValue::I64(v) => Ok((TYPE_I64, v.to_le_bytes().to_vec())),
        FieldValue::F64(v) => Ok((TYPE_F64, v.to_le_bytes().to_vec())),
        FieldValue::InlineBytes(bytes) => {
            if bytes.len() > u16::MAX as usize {
                return Err(PackedDocError::FieldDataTooLarge {
                    field_id,
                    len: bytes.len(),
                });
            }
            let mut data = Vec::with_capacity(bytes.len() + 2);
            data.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            data.extend_from_slice(&bytes);
            Ok((TYPE_STR_INLINE, data))
        }
        FieldValue::DictRef(id) => Ok((TYPE_STR_DICTREF, id.to_le_bytes().to_vec())),
        FieldValue::ArrayBytes(bytes) => {
            if bytes.len() > u16::MAX as usize {
                return Err(PackedDocError::FieldDataTooLarge {
                    field_id,
                    len: bytes.len(),
                });
            }
            let mut data = Vec::with_capacity(bytes.len() + 2);
            data.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            data.extend_from_slice(&bytes);
            Ok((TYPE_ARRAY, data))
        }
    }
}

fn decode_field_value(
    tag: u8,
    data: &[u8],
    field_id: FieldId,
) -> Result<FieldValue, PackedDocError> {
    match tag {
        TYPE_NULL => {
            if !data.is_empty() {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "null field must not have payload bytes",
                });
            }
            Ok(FieldValue::Null)
        }
        TYPE_BOOL_FALSE => {
            if !data.is_empty() {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "bool field must not have payload bytes",
                });
            }
            Ok(FieldValue::Bool(false))
        }
        TYPE_BOOL_TRUE => {
            if !data.is_empty() {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "bool field must not have payload bytes",
                });
            }
            Ok(FieldValue::Bool(true))
        }
        TYPE_I64 => {
            if data.len() != 8 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "i64 field payload must be exactly 8 bytes",
                });
            }
            let bytes: [u8; 8] = data
                .try_into()
                .map_err(|_| PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "i64 conversion failed",
                })?;
            Ok(FieldValue::I64(i64::from_le_bytes(bytes)))
        }
        TYPE_F64 => {
            if data.len() != 8 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "f64 field payload must be exactly 8 bytes",
                });
            }
            let bytes: [u8; 8] = data
                .try_into()
                .map_err(|_| PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "f64 conversion failed",
                })?;
            Ok(FieldValue::F64(f64::from_le_bytes(bytes)))
        }
        TYPE_STR_INLINE => {
            if data.len() < 2 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "inline value missing length prefix",
                });
            }
            let len = u16::from_le_bytes([data[0], data[1]]) as usize;
            if data.len() != len + 2 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "inline value length prefix mismatch",
                });
            }
            Ok(FieldValue::InlineBytes(data[2..].to_vec()))
        }
        TYPE_STR_DICTREF => {
            if data.len() != 4 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "dict ref payload must be 4 bytes",
                });
            }
            let bytes: [u8; 4] = data
                .try_into()
                .map_err(|_| PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "dict ref conversion failed",
                })?;
            Ok(FieldValue::DictRef(u32::from_le_bytes(bytes)))
        }
        TYPE_ARRAY => {
            if data.len() < 2 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "array value missing length prefix",
                });
            }
            let len = u16::from_le_bytes([data[0], data[1]]) as usize;
            if data.len() != len + 2 {
                return Err(PackedDocError::InvalidFieldData {
                    field_id,
                    reason: "array value length prefix mismatch",
                });
            }
            Ok(FieldValue::ArrayBytes(data[2..].to_vec()))
        }
        _ => Err(PackedDocError::UnknownTypeTag(tag)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_read_round_trip() {
        let mut builder = PackedDocBuilder::new(1);
        builder
            .add_field(2, FieldValue::I64(42))
            .expect("field should be added");
        builder
            .add_field(1, FieldValue::InlineBytes(b"augustus".to_vec()))
            .expect("field should be added");
        builder
            .add_field(4, FieldValue::DictRef(7))
            .expect("field should be added");
        builder
            .add_field(3, FieldValue::Bool(true))
            .expect("field should be added");

        let doc = builder.build(123).expect("doc should build");

        assert_eq!(doc.version().expect("version"), 1);
        assert_eq!(doc.updated_at().expect("updated_at"), 123);
        assert_eq!(doc.field_count().expect("field_count"), 4);
        assert_eq!(
            doc.read_field(1).expect("read should succeed"),
            Some(FieldValue::InlineBytes(b"augustus".to_vec()))
        );
        assert_eq!(
            doc.read_field(2).expect("read should succeed"),
            Some(FieldValue::I64(42))
        );
        assert_eq!(
            doc.read_field(3).expect("read should succeed"),
            Some(FieldValue::Bool(true))
        );
        assert_eq!(
            doc.read_field(4).expect("read should succeed"),
            Some(FieldValue::DictRef(7))
        );
        assert_eq!(doc.read_field(9).expect("read should succeed"), None);
    }

    #[test]
    fn iterator_returns_sorted_fields() {
        let mut builder = PackedDocBuilder::new(1);
        builder
            .add_field(4, FieldValue::Null)
            .expect("field should be added");
        builder
            .add_field(2, FieldValue::Bool(false))
            .expect("field should be added");
        builder
            .add_field(3, FieldValue::F64(1.5))
            .expect("field should be added");

        let doc = builder.build(0).expect("doc should build");
        let fields: Vec<(FieldId, FieldValue)> = doc
            .iter_fields()
            .expect("iterator should be created")
            .collect::<Result<_, _>>()
            .expect("iteration should decode");
        let field_ids: Vec<FieldId> = fields.iter().map(|(id, _)| *id).collect();
        assert_eq!(field_ids, vec![2, 3, 4]);
    }

    #[test]
    fn duplicate_field_ids_are_rejected() {
        let mut builder = PackedDocBuilder::new(1);
        builder
            .add_field(1, FieldValue::Null)
            .expect("field should be added");
        builder
            .add_field(1, FieldValue::Bool(false))
            .expect("field should be added");

        let err = builder.build(0).expect_err("duplicate fields must fail");
        assert_eq!(err, PackedDocError::DuplicateFieldId(1));
    }

    #[test]
    fn oversized_field_payload_is_rejected() {
        let mut builder = PackedDocBuilder::new(1);
        let large = vec![0u8; 70_000];
        let err = builder
            .add_field(2, FieldValue::InlineBytes(large))
            .expect_err("oversized field must fail");
        assert_eq!(
            err,
            PackedDocError::FieldDataTooLarge {
                field_id: 2,
                len: 70_000
            }
        );
    }

    #[test]
    fn malformed_doc_is_rejected() {
        let err = PackedDoc::from_bytes(vec![1, 2, 3]).expect_err("malformed doc should fail");
        assert_eq!(err, PackedDocError::Malformed("buffer shorter than header"));
    }
}
