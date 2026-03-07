//! Stress and property tests for the kora-protocol RESP parser and serializer.

use bytes::BytesMut;
use kora_core::command::CommandResponse;
use kora_protocol::{serialize_response, RespParser, RespValue};
use rand::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialize a CommandResponse to bytes.
fn serialize(resp: &CommandResponse) -> Vec<u8> {
    let mut buf = BytesMut::new();
    serialize_response(resp, &mut buf);
    buf.to_vec()
}

/// Generate a random CommandResponse (no \r or \n in strings to keep RESP valid).
fn random_command_response(rng: &mut impl Rng, depth: u32) -> CommandResponse {
    if depth > 3 {
        // Limit recursion depth — return a leaf variant.
        return CommandResponse::Integer(rng.gen_range(-1000..1000));
    }
    match rng.gen_range(0u8..7) {
        0 => CommandResponse::Ok,
        1 => CommandResponse::Nil,
        2 => CommandResponse::Integer(rng.gen_range(i64::MIN / 2..i64::MAX / 2)),
        3 => {
            let len = rng.gen_range(0..128);
            let data: Vec<u8> = (0..len).map(|_| rng.gen_range(0..=255)).collect();
            CommandResponse::BulkString(data)
        }
        4 => {
            // SimpleString — must not contain \r or \n
            let len = rng.gen_range(0..64);
            let s: String = (0..len)
                .map(|_| {
                    let c = rng.gen_range(0x20u8..0x7F);
                    c as char
                })
                .collect();
            CommandResponse::SimpleString(s)
        }
        5 => {
            let len = rng.gen_range(0..6);
            let items: Vec<CommandResponse> = (0..len)
                .map(|_| random_command_response(rng, depth + 1))
                .collect();
            CommandResponse::Array(items)
        }
        6 => {
            // Error — must not contain \r or \n
            let len = rng.gen_range(1..64);
            let s: String = (0..len)
                .map(|_| {
                    let c = rng.gen_range(0x20u8..0x7F);
                    c as char
                })
                .collect();
            CommandResponse::Error(s)
        }
        _ => unreachable!(),
    }
}

/// Convert a parsed RespValue back into a CommandResponse for comparison.
/// This is a best-effort mapping since the RESP wire format loses some
/// distinctions (e.g., Ok vs SimpleString("OK")).
fn resp_to_command_response(val: &RespValue) -> CommandResponse {
    match val {
        RespValue::SimpleString(data) => {
            let s = String::from_utf8_lossy(data).to_string();
            if s == "OK" {
                CommandResponse::Ok
            } else {
                CommandResponse::SimpleString(s)
            }
        }
        RespValue::Error(data) => CommandResponse::Error(String::from_utf8_lossy(data).to_string()),
        RespValue::Integer(n) => CommandResponse::Integer(*n),
        RespValue::BulkString(None) => CommandResponse::Nil,
        RespValue::BulkString(Some(data)) => CommandResponse::BulkString(data.clone()),
        RespValue::Array(None) => {
            // Null array — no direct CommandResponse equivalent, treat as empty
            CommandResponse::Array(vec![])
        }
        RespValue::Array(Some(items)) => {
            let converted: Vec<CommandResponse> =
                items.iter().map(resp_to_command_response).collect();
            CommandResponse::Array(converted)
        }
        RespValue::Null => CommandResponse::Nil,
        RespValue::Double(d) => CommandResponse::Double(*d),
        RespValue::Boolean(b) => CommandResponse::Boolean(*b),
        RespValue::Map(pairs) => {
            let converted: Vec<(CommandResponse, CommandResponse)> = pairs
                .iter()
                .map(|(k, v)| (resp_to_command_response(k), resp_to_command_response(v)))
                .collect();
            CommandResponse::Map(converted)
        }
        RespValue::Set(items) => {
            let converted: Vec<CommandResponse> =
                items.iter().map(resp_to_command_response).collect();
            CommandResponse::Set(converted)
        }
        RespValue::BigNumber(data) => CommandResponse::BulkString(data.clone()),
        RespValue::VerbatimString { data, .. } => CommandResponse::BulkString(data.clone()),
        RespValue::Push(items) => {
            let converted: Vec<CommandResponse> =
                items.iter().map(resp_to_command_response).collect();
            CommandResponse::Array(converted)
        }
    }
}

// ---------------------------------------------------------------------------
// 1. Fuzz-like RESP parser test — random bytes must never panic
// ---------------------------------------------------------------------------

#[test]
fn fuzz_random_bytes_never_panic() {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);
    for _ in 0..10_000 {
        let len = rng.gen_range(0..256);
        let data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
        let mut parser = RespParser::new();
        parser.feed(&data);
        // We don't care whether it returns Ok or Err — only that it doesn't panic.
        let _ = parser.try_parse();
    }
}

#[test]
fn fuzz_random_bytes_with_resp_prefix_never_panic() {
    let prefixes: &[u8] = b"+*$:-";
    let mut rng = StdRng::seed_from_u64(0xCAFE_BABE);
    for _ in 0..10_000 {
        let mut data = vec![prefixes[rng.gen_range(0..prefixes.len())]];
        let tail_len = rng.gen_range(0..128);
        for _ in 0..tail_len {
            data.push(rng.gen());
        }
        let mut parser = RespParser::new();
        parser.feed(&data);
        let _ = parser.try_parse();
    }
}

// ---------------------------------------------------------------------------
// 2. Roundtrip test — serialize then parse, verify consistency
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_random_command_responses() {
    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..2_000 {
        let original = random_command_response(&mut rng, 0);
        let wire = serialize(&original);

        let mut parser = RespParser::new();
        parser.feed(&wire);
        let parsed = parser
            .try_parse()
            .expect("serialized data should parse without error")
            .expect("serialized data should be a complete frame");

        let roundtripped = resp_to_command_response(&parsed);

        // CommandResponse::Ok serializes as "+OK\r\n" which parses to
        // SimpleString("OK"). Our mapping converts it back to Ok, so this
        // should hold:
        assert_eq!(
            original, roundtripped,
            "roundtrip failed.\n  original: {:?}\n  wire: {:?}\n  parsed: {:?}\n  roundtripped: {:?}",
            original, wire, parsed, roundtripped,
        );

        // Confirm no leftover data in the parser buffer.
        assert_eq!(parser.buffer_len(), 0, "leftover data in parser buffer");
    }
}

// ---------------------------------------------------------------------------
// 3. Incremental parsing test — byte-by-byte must yield same result
// ---------------------------------------------------------------------------

#[test]
fn incremental_byte_by_byte_matches_bulk_parse() {
    let test_frames: Vec<Vec<u8>> = vec![
        b"+OK\r\n".to_vec(),
        b"-ERR something\r\n".to_vec(),
        b":12345\r\n".to_vec(),
        b"$5\r\nhello\r\n".to_vec(),
        b"$0\r\n\r\n".to_vec(),
        b"$-1\r\n".to_vec(),
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec(),
        b"*-1\r\n".to_vec(),
        b"*3\r\n:1\r\n:2\r\n:3\r\n".to_vec(),
        b"*2\r\n*1\r\n$4\r\ntest\r\n:99\r\n".to_vec(),
    ];

    // Also add some randomly generated frames.
    let mut rng = StdRng::seed_from_u64(999);
    let mut all_frames = test_frames;
    for _ in 0..200 {
        let resp = random_command_response(&mut rng, 0);
        all_frames.push(serialize(&resp));
    }

    for frame in &all_frames {
        // Bulk parse
        let bulk_result = {
            let mut parser = RespParser::new();
            parser.feed(frame);
            parser.try_parse()
        };

        // Byte-by-byte parse
        let incremental_result = {
            let mut parser = RespParser::new();
            let mut result = None;
            for (i, &byte) in frame.iter().enumerate() {
                parser.feed(&[byte]);
                match parser.try_parse() {
                    Ok(Some(val)) => {
                        result = Some(Ok(Some(val)));
                        // All remaining bytes should already be consumed or belong
                        // to a subsequent frame. For a single frame, the rest
                        // should be empty.
                        assert_eq!(parser.buffer_len(), 0, "unexpected leftover at byte {}", i);
                        break;
                    }
                    Ok(None) => { /* need more data */ }
                    Err(e) => {
                        result = Some(Err(e));
                        break;
                    }
                }
            }
            result.unwrap_or(Ok(None))
        };

        assert_eq!(
            format!("{:?}", bulk_result),
            format!("{:?}", incremental_result),
            "byte-by-byte result differs from bulk parse for frame {:?}",
            frame,
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Pipeline stress test — concatenate many frames, parse all correctly
// ---------------------------------------------------------------------------

#[test]
fn pipeline_many_concatenated_frames() {
    let mut rng = StdRng::seed_from_u64(0x1234);
    let count = 500;

    let mut expected: Vec<CommandResponse> = Vec::with_capacity(count);
    let mut wire = Vec::new();

    for _ in 0..count {
        let resp = random_command_response(&mut rng, 0);
        wire.extend_from_slice(&serialize(&resp));
        expected.push(resp);
    }

    let mut parser = RespParser::new();
    parser.feed(&wire);

    for (i, exp) in expected.iter().enumerate() {
        let parsed = parser
            .try_parse()
            .unwrap_or_else(|e| panic!("parse error at frame {}: {:?}", i, e))
            .unwrap_or_else(|| panic!("incomplete data at frame {}", i));
        let converted = resp_to_command_response(&parsed);
        assert_eq!(
            *exp, converted,
            "mismatch at frame {}.\n  expected: {:?}\n  got: {:?}",
            i, exp, converted,
        );
    }

    // No more frames.
    assert!(
        parser.try_parse().unwrap().is_none(),
        "expected no more frames"
    );
    assert_eq!(parser.buffer_len(), 0, "leftover bytes in buffer");
}

#[test]
fn pipeline_standard_commands() {
    // Simulate a realistic pipeline of Redis commands.
    let commands: Vec<&[u8]> = vec![
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
        b"*1\r\n$4\r\nPING\r\n",
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n",
    ];

    let mut combined = Vec::new();
    for cmd in &commands {
        combined.extend_from_slice(cmd);
    }

    let mut parser = RespParser::new();
    parser.feed(&combined);

    for (i, _) in commands.iter().enumerate() {
        let val = parser
            .try_parse()
            .unwrap_or_else(|e| panic!("error at command {}: {:?}", i, e))
            .unwrap_or_else(|| panic!("incomplete at command {}", i));
        // Each command should be an Array.
        match val {
            RespValue::Array(Some(_)) => {}
            other => panic!("expected array at command {}, got {:?}", i, other),
        }
    }

    assert!(parser.try_parse().unwrap().is_none());
}

// ---------------------------------------------------------------------------
// 5. Large payload test — big bulk strings and deeply nested arrays
// ---------------------------------------------------------------------------

#[test]
fn large_bulk_string() {
    let size = 1_000_000; // 1 MB
    let payload: Vec<u8> = vec![b'x'; size];
    let resp = CommandResponse::BulkString(payload.clone());
    let wire = serialize(&resp);

    let mut parser = RespParser::new();
    parser.feed(&wire);
    let parsed = parser.try_parse().unwrap().unwrap();
    assert_eq!(parsed, RespValue::BulkString(Some(payload)));
    assert_eq!(parser.buffer_len(), 0);
}

#[test]
fn large_bulk_string_with_binary_data() {
    // Bulk strings can contain arbitrary bytes including \r\n sequences.
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    let size = 100_000;
    let payload: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
    let resp = CommandResponse::BulkString(payload.clone());
    let wire = serialize(&resp);

    let mut parser = RespParser::new();
    parser.feed(&wire);
    let parsed = parser.try_parse().unwrap().unwrap();
    assert_eq!(parsed, RespValue::BulkString(Some(payload)));
}

#[test]
fn deeply_nested_arrays() {
    // Build an array nested 50 levels deep, with a single integer at the leaf.
    fn build_nested(depth: u32) -> CommandResponse {
        if depth == 0 {
            CommandResponse::Integer(42)
        } else {
            CommandResponse::Array(vec![build_nested(depth - 1)])
        }
    }

    let depth = 50;
    let resp = build_nested(depth);
    let wire = serialize(&resp);

    let mut parser = RespParser::new();
    parser.feed(&wire);
    let parsed = parser.try_parse().unwrap().unwrap();
    let roundtripped = resp_to_command_response(&parsed);
    assert_eq!(resp, roundtripped);
}

#[test]
fn wide_array() {
    // Array with 10,000 integer elements.
    let count = 10_000;
    let items: Vec<CommandResponse> = (0..count).map(CommandResponse::Integer).collect();
    let resp = CommandResponse::Array(items);
    let wire = serialize(&resp);

    let mut parser = RespParser::new();
    parser.feed(&wire);
    let parsed = parser.try_parse().unwrap().unwrap();

    match &parsed {
        RespValue::Array(Some(arr)) => assert_eq!(arr.len(), count as usize),
        other => panic!("expected array, got {:?}", other),
    }

    let roundtripped = resp_to_command_response(&parsed);
    assert_eq!(resp, roundtripped);
}

#[test]
fn large_array_of_bulk_strings() {
    let count = 1_000;
    let items: Vec<CommandResponse> = (0..count)
        .map(|i| CommandResponse::BulkString(format!("value-{:04}", i).into_bytes()))
        .collect();
    let resp = CommandResponse::Array(items);
    let wire = serialize(&resp);

    let mut parser = RespParser::new();
    parser.feed(&wire);
    let parsed = parser.try_parse().unwrap().unwrap();
    let roundtripped = resp_to_command_response(&parsed);
    assert_eq!(resp, roundtripped);
    assert_eq!(parser.buffer_len(), 0);
}
