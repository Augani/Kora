use super::*;

pub(super) fn parse_doc_create(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() == 1 {
        return Ok(Command::DocCreate {
            collection: extract_bytes(&args[0])?,
            compression: None,
        });
    }

    if args.len() != 3 {
        return Err(ProtocolError::WrongArity("DOC.CREATE".into()));
    }

    if !extract_string(&args[1])?.eq_ignore_ascii_case(b"COMPRESSION") {
        return Err(ProtocolError::InvalidData(
            "DOC.CREATE only supports optional COMPRESSION clause".into(),
        ));
    }

    Ok(Command::DocCreate {
        collection: extract_bytes(&args[0])?,
        compression: Some(extract_bytes(&args[2])?),
    })
}

pub(super) fn parse_doc_get(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() == 2 {
        return Ok(Command::DocGet {
            collection: extract_bytes(&args[0])?,
            doc_id: extract_bytes(&args[1])?,
            fields: Vec::new(),
        });
    }

    if args.len() < 4 {
        return Err(ProtocolError::WrongArity("DOC.GET".into()));
    }

    if !extract_string(&args[2])?.eq_ignore_ascii_case(b"FIELDS") {
        return Err(ProtocolError::InvalidData(
            "DOC.GET optional arguments must start with FIELDS".into(),
        ));
    }

    let fields = args[3..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;

    if fields.is_empty() {
        return Err(ProtocolError::WrongArity("DOC.GET".into()));
    }

    Ok(Command::DocGet {
        collection: extract_bytes(&args[0])?,
        doc_id: extract_bytes(&args[1])?,
        fields,
    })
}

pub(super) fn parse_doc_mset(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(ProtocolError::WrongArity("DOC.MSET".into()));
    }

    let collection = extract_bytes(&args[0])?;
    let entries = args[1..]
        .chunks(2)
        .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
        .collect::<Result<Vec<_>, ProtocolError>>()?;

    Ok(Command::DocMSet {
        collection,
        entries,
    })
}

pub(super) fn parse_doc_mget(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("DOC.MGET".into()));
    }

    let collection = extract_bytes(&args[0])?;
    let doc_ids = args[1..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Command::DocMGet {
        collection,
        doc_ids,
    })
}

pub(super) fn parse_doc_update(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 4 {
        return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
    }

    let collection = extract_bytes(&args[0])?;
    let doc_id = extract_bytes(&args[1])?;
    let mut mutations = Vec::new();
    let mut i = 2usize;

    while i < args.len() {
        let op = extract_string(&args[i])?.to_ascii_uppercase();
        i += 1;
        match op.as_slice() {
            b"SET" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
                }
                mutations.push(DocUpdateMutation::Set {
                    path: extract_bytes(&args[i])?,
                    value: extract_bytes(&args[i + 1])?,
                });
                i += 2;
            }
            b"DEL" => {
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
                }
                mutations.push(DocUpdateMutation::Del {
                    path: extract_bytes(&args[i])?,
                });
                i += 1;
            }
            b"INCR" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
                }
                mutations.push(DocUpdateMutation::Incr {
                    path: extract_bytes(&args[i])?,
                    delta: parse_f64(&args[i + 1])?,
                });
                i += 2;
            }
            b"PUSH" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
                }
                mutations.push(DocUpdateMutation::Push {
                    path: extract_bytes(&args[i])?,
                    value: extract_bytes(&args[i + 1])?,
                });
                i += 2;
            }
            b"PULL" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.UPDATE".into()));
                }
                mutations.push(DocUpdateMutation::Pull {
                    path: extract_bytes(&args[i])?,
                    value: extract_bytes(&args[i + 1])?,
                });
                i += 2;
            }
            _ => {
                return Err(ProtocolError::InvalidData(
                    "DOC.UPDATE expects SET|DEL|INCR|PUSH|PULL clauses".into(),
                ));
            }
        }
    }

    Ok(Command::DocUpdate {
        collection,
        doc_id,
        mutations,
    })
}

pub(super) fn parse_doc_createindex(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_arity("DOC.CREATEINDEX", args, 3)?;
    let index_type_raw = extract_string(&args[2])?.to_ascii_lowercase();
    match index_type_raw.as_slice() {
        b"hash" | b"sorted" | b"array" | b"unique" => {}
        _ => {
            return Err(ProtocolError::InvalidData(
                "DOC.CREATEINDEX type must be hash|sorted|array|unique".into(),
            ));
        }
    }
    Ok(Command::DocCreateIndex {
        collection: extract_bytes(&args[0])?,
        field: extract_bytes(&args[1])?,
        index_type: index_type_raw,
    })
}

pub(super) fn parse_doc_find(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("DOC.FIND".into()));
    }

    let collection = extract_bytes(&args[0])?;
    let where_kw = extract_string(&args[1])?.to_ascii_uppercase();
    if where_kw.as_slice() != b"WHERE" {
        return Err(ProtocolError::InvalidData(
            "DOC.FIND expects WHERE keyword".into(),
        ));
    }

    let mut where_parts: Vec<Vec<u8>> = Vec::new();
    let mut idx = 2usize;
    while idx < args.len() {
        let token = extract_string(&args[idx])?.to_ascii_uppercase();
        if token.as_slice() == b"PROJECT"
            || token.as_slice() == b"LIMIT"
            || token.as_slice() == b"OFFSET"
            || token.as_slice() == b"ORDER"
        {
            break;
        }
        where_parts.push(extract_bytes(&args[idx])?);
        idx += 1;
    }

    if where_parts.is_empty() {
        return Err(ProtocolError::InvalidData(
            "DOC.FIND WHERE clause is empty".into(),
        ));
    }

    let where_clause = where_parts.join(&b' ');

    let mut fields: Vec<Vec<u8>> = Vec::new();
    let mut limit: Option<usize> = None;
    let mut offset: usize = 0;
    let mut order_by: Option<Vec<u8>> = None;
    let mut order_desc: bool = false;

    while idx < args.len() {
        let kw = extract_string(&args[idx])?.to_ascii_uppercase();
        idx += 1;
        match kw.as_slice() {
            b"ORDER" => {
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                let by_kw = extract_string(&args[idx])?.to_ascii_uppercase();
                if by_kw.as_slice() != b"BY" {
                    return Err(ProtocolError::InvalidData(
                        "DOC.FIND ORDER expects BY keyword".into(),
                    ));
                }
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                order_by = Some(extract_bytes(&args[idx])?);
                idx += 1;
                if idx < args.len() {
                    let dir = extract_string(&args[idx])?.to_ascii_uppercase();
                    match dir.as_slice() {
                        b"ASC" => {
                            idx += 1;
                        }
                        b"DESC" => {
                            order_desc = true;
                            idx += 1;
                        }
                        _ => {}
                    }
                }
            }
            b"PROJECT" => {
                let project_start = idx;
                while idx < args.len() {
                    let peek = extract_string(&args[idx])?.to_ascii_uppercase();
                    if peek.as_slice() == b"LIMIT"
                        || peek.as_slice() == b"OFFSET"
                        || peek.as_slice() == b"ORDER"
                    {
                        break;
                    }
                    fields.push(extract_bytes(&args[idx])?);
                    idx += 1;
                }
                if idx == project_start {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
            }
            b"LIMIT" => {
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                limit = Some(parse_u64(&args[idx])? as usize);
                idx += 1;
            }
            b"OFFSET" => {
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                offset = parse_u64(&args[idx])? as usize;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidData(
                    "DOC.FIND unexpected keyword after WHERE clause".into(),
                ));
            }
        }
    }

    Ok(Command::DocFind {
        collection,
        where_clause,
        fields,
        limit,
        offset,
        order_by,
        order_desc,
    })
}

pub(super) fn parse_doc_count(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("DOC.COUNT".into()));
    }

    let collection = extract_bytes(&args[0])?;
    let where_kw = extract_string(&args[1])?.to_ascii_uppercase();
    if where_kw.as_slice() != b"WHERE" {
        return Err(ProtocolError::InvalidData(
            "DOC.COUNT expects WHERE keyword".into(),
        ));
    }

    let where_parts: Vec<Vec<u8>> = args[2..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;

    if where_parts.is_empty() {
        return Err(ProtocolError::InvalidData(
            "DOC.COUNT WHERE clause is empty".into(),
        ));
    }

    let where_clause = where_parts.join(&b' ');

    Ok(Command::DocCount {
        collection,
        where_clause,
    })
}
