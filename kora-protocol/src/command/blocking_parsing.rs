use super::*;

pub(super) fn parse_blpop(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BLPOP", args, 2)?;
    let timeout = parse_f64(args.last().unwrap())?;
    let keys = args[..args.len() - 1]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::BLPop { keys, timeout })
}

pub(super) fn parse_brpop(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BRPOP", args, 2)?;
    let timeout = parse_f64(args.last().unwrap())?;
    let keys = args[..args.len() - 1]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::BRPop { keys, timeout })
}

pub(super) fn parse_blmove(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_arity("BLMOVE", args, 5)?;
    let source = extract_bytes(&args[0])?;
    let destination = extract_bytes(&args[1])?;
    let from_dir = extract_string(&args[2])?.to_ascii_uppercase();
    let to_dir = extract_string(&args[3])?.to_ascii_uppercase();
    let from_left = match from_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => {
            return Err(ProtocolError::InvalidData(
                "ERR syntax error, LEFT or RIGHT required".into(),
            ))
        }
    };
    let to_left = match to_dir.as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => {
            return Err(ProtocolError::InvalidData(
                "ERR syntax error, LEFT or RIGHT required".into(),
            ))
        }
    };
    let timeout = parse_f64(&args[4])?;
    Ok(Command::BLMove {
        source,
        destination,
        from_left,
        to_left,
        timeout,
    })
}

pub(super) fn parse_bzpopmin(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BZPOPMIN", args, 2)?;
    let timeout = parse_f64(args.last().unwrap())?;
    let keys = args[..args.len() - 1]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::BZPopMin { keys, timeout })
}

pub(super) fn parse_bzpopmax(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BZPOPMAX", args, 2)?;
    let timeout = parse_f64(args.last().unwrap())?;
    let keys = args[..args.len() - 1]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::BZPopMax { keys, timeout })
}
