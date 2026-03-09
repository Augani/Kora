//! WHERE expression parser for document queries.

use thiserror::Error;

/// Parsed WHERE clause expression AST.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Equality comparison: `field = value`.
    Eq(String, ExprValue),
    /// Inequality comparison: `field != value`.
    Neq(String, ExprValue),
    /// Greater-than comparison: `field > number`.
    Gt(String, f64),
    /// Greater-than-or-equal comparison: `field >= number`.
    Gte(String, f64),
    /// Less-than comparison: `field < number`.
    Lt(String, f64),
    /// Less-than-or-equal comparison: `field <= number`.
    Lte(String, f64),
    /// Array membership test: `field CONTAINS value`.
    Contains(String, ExprValue),
    /// Logical AND of two expressions.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR of two expressions.
    Or(Box<Expr>, Box<Expr>),
}

/// A literal value in a WHERE expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprValue {
    /// A string literal (single- or double-quoted).
    String(String),
    /// A numeric literal.
    Number(f64),
    /// A boolean literal (`true` or `false`).
    Bool(bool),
    /// The null literal.
    Null,
}

/// Errors that can occur while parsing a WHERE expression.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExprError {
    /// The expression ended unexpectedly.
    #[error("unexpected end of expression")]
    UnexpectedEnd,
    /// An unexpected token was encountered.
    #[error("unexpected token: {0}")]
    UnexpectedToken(String),
    /// A string literal was not properly terminated.
    #[error("unterminated string literal")]
    UnterminatedString,
    /// A numeric literal could not be parsed.
    #[error("invalid number: {0}")]
    InvalidNumber(String),
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    NumberLit(f64),
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Contains,
    True,
    False,
    Null,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, ExprError> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            tokens.push(self.next_token()?);
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token, ExprError> {
        self.skip_whitespace();

        let byte = match self.peek_byte() {
            Some(b) => b,
            None => return Err(ExprError::UnexpectedEnd),
        };

        match byte {
            b'"' | b'\'' => self.lex_string(),
            b'!' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::Neq)
                } else {
                    Err(ExprError::UnexpectedToken("!".into()))
                }
            }
            b'=' => {
                self.pos += 1;
                Ok(Token::Eq)
            }
            b'>' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            b'<' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::Lte)
                } else {
                    Ok(Token::Lt)
                }
            }
            b'-' => self.lex_number(),
            b if b.is_ascii_digit() => self.lex_number(),
            b if b.is_ascii_alphabetic() || b == b'_' => self.lex_ident_or_keyword(),
            _ => Err(ExprError::UnexpectedToken(
                String::from_utf8_lossy(&self.input[self.pos..self.pos + 1]).into_owned(),
            )),
        }
    }

    fn lex_string(&mut self) -> Result<Token, ExprError> {
        let quote = self.input[self.pos];
        self.pos += 1;
        let start = self.pos;

        while self.pos < self.input.len() {
            if self.input[self.pos] == quote {
                let value = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
                self.pos += 1;
                return Ok(Token::StringLit(value));
            }
            self.pos += 1;
        }

        Err(ExprError::UnterminatedString)
    }

    fn lex_number(&mut self) -> Result<Token, ExprError> {
        let start = self.pos;

        if self.peek_byte() == Some(b'-') {
            self.pos += 1;
        }

        if self.pos >= self.input.len() || !self.input[self.pos].is_ascii_digit() {
            let fragment = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
            return Err(ExprError::InvalidNumber(fragment));
        }

        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        if self.pos < self.input.len() && self.input[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        let fragment = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
        let value = fragment
            .parse::<f64>()
            .map_err(|_| ExprError::InvalidNumber(fragment))?;
        Ok(Token::NumberLit(value))
    }

    fn lex_ident_or_keyword(&mut self) -> Result<Token, ExprError> {
        let start = self.pos;

        while self.pos < self.input.len()
            && (self.input[self.pos].is_ascii_alphanumeric()
                || self.input[self.pos] == b'_'
                || self.input[self.pos] == b'.')
        {
            self.pos += 1;
        }

        let word = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
        let upper = word.to_ascii_uppercase();

        match upper.as_str() {
            "AND" => Ok(Token::And),
            "OR" => Ok(Token::Or),
            "CONTAINS" => Ok(Token::Contains),
            "TRUE" => Ok(Token::True),
            "FALSE" => Ok(Token::False),
            "NULL" => Ok(Token::Null),
            _ => Ok(Token::Ident(word)),
        }
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let token = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, ExprError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_and()?;

        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_compare()?;

        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_compare()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_compare(&mut self) -> Result<Expr, ExprError> {
        let field = match self.advance() {
            Some(Token::Ident(name)) => name,
            Some(other) => return Err(ExprError::UnexpectedToken(format!("{other:?}"))),
            None => return Err(ExprError::UnexpectedEnd),
        };

        let op = match self.advance() {
            Some(t) => t,
            None => return Err(ExprError::UnexpectedEnd),
        };

        match op {
            Token::Eq => {
                let value = self.parse_value()?;
                Ok(Expr::Eq(field, value))
            }
            Token::Neq => {
                let value = self.parse_value()?;
                Ok(Expr::Neq(field, value))
            }
            Token::Gt => {
                let n = self.parse_number_value()?;
                Ok(Expr::Gt(field, n))
            }
            Token::Gte => {
                let n = self.parse_number_value()?;
                Ok(Expr::Gte(field, n))
            }
            Token::Lt => {
                let n = self.parse_number_value()?;
                Ok(Expr::Lt(field, n))
            }
            Token::Lte => {
                let n = self.parse_number_value()?;
                Ok(Expr::Lte(field, n))
            }
            Token::Contains => {
                let value = self.parse_value()?;
                Ok(Expr::Contains(field, value))
            }
            other => Err(ExprError::UnexpectedToken(format!("{other:?}"))),
        }
    }

    fn parse_value(&mut self) -> Result<ExprValue, ExprError> {
        match self.advance() {
            Some(Token::StringLit(s)) => Ok(ExprValue::String(s)),
            Some(Token::NumberLit(n)) => Ok(ExprValue::Number(n)),
            Some(Token::True) => Ok(ExprValue::Bool(true)),
            Some(Token::False) => Ok(ExprValue::Bool(false)),
            Some(Token::Null) => Ok(ExprValue::Null),
            Some(other) => Err(ExprError::UnexpectedToken(format!("{other:?}"))),
            None => Err(ExprError::UnexpectedEnd),
        }
    }

    fn parse_number_value(&mut self) -> Result<f64, ExprError> {
        match self.advance() {
            Some(Token::NumberLit(n)) => Ok(n),
            Some(other) => Err(ExprError::UnexpectedToken(format!("{other:?}"))),
            None => Err(ExprError::UnexpectedEnd),
        }
    }
}

/// Parse a WHERE clause string into an [`Expr`] AST.
pub fn parse_where(input: &str) -> Result<Expr, ExprError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize()?;

    if tokens.is_empty() {
        return Err(ExprError::UnexpectedEnd);
    }

    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;

    if parser.pos < parser.tokens.len() {
        return Err(ExprError::UnexpectedToken(format!(
            "{:?}",
            parser.tokens[parser.pos]
        )));
    }

    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_equality() {
        let expr = parse_where(r#"city = "Accra""#).unwrap();
        assert_eq!(
            expr,
            Expr::Eq("city".into(), ExprValue::String("Accra".into()))
        );
    }

    #[test]
    fn parse_single_quoted_string() {
        let expr = parse_where("city = 'Accra'").unwrap();
        assert_eq!(
            expr,
            Expr::Eq("city".into(), ExprValue::String("Accra".into()))
        );
    }

    #[test]
    fn parse_numeric_comparison() {
        let expr = parse_where("age >= 25").unwrap();
        assert_eq!(expr, Expr::Gte("age".into(), 25.0));
    }

    #[test]
    fn parse_negative_number() {
        let expr = parse_where("temp > -10").unwrap();
        assert_eq!(expr, Expr::Gt("temp".into(), -10.0));
    }

    #[test]
    fn parse_float_number() {
        let expr = parse_where("score >= 3.14").unwrap();
        assert_eq!(expr, Expr::Gte("score".into(), 3.14));
    }

    #[test]
    fn parse_boolean_value() {
        let expr = parse_where("active = true").unwrap();
        assert_eq!(expr, Expr::Eq("active".into(), ExprValue::Bool(true)));
    }

    #[test]
    fn parse_null_value() {
        let expr = parse_where("deleted = null").unwrap();
        assert_eq!(expr, Expr::Eq("deleted".into(), ExprValue::Null));
    }

    #[test]
    fn parse_not_equal() {
        let expr = parse_where(r#"status != "inactive""#).unwrap();
        assert_eq!(
            expr,
            Expr::Neq("status".into(), ExprValue::String("inactive".into()))
        );
    }

    #[test]
    fn parse_contains() {
        let expr = parse_where(r#"tags CONTAINS "rust""#).unwrap();
        assert_eq!(
            expr,
            Expr::Contains("tags".into(), ExprValue::String("rust".into()))
        );
    }

    #[test]
    fn parse_dotted_path() {
        let expr = parse_where(r#"address.city = "Accra""#).unwrap();
        assert_eq!(
            expr,
            Expr::Eq("address.city".into(), ExprValue::String("Accra".into()))
        );
    }

    #[test]
    fn parse_and() {
        let expr = parse_where(r#"city = "Accra" AND age >= 25"#).unwrap();
        assert_eq!(
            expr,
            Expr::And(
                Box::new(Expr::Eq("city".into(), ExprValue::String("Accra".into()))),
                Box::new(Expr::Gte("age".into(), 25.0)),
            )
        );
    }

    #[test]
    fn parse_or() {
        let expr = parse_where(r#"city = "Accra" OR city = "Lagos""#).unwrap();
        assert_eq!(
            expr,
            Expr::Or(
                Box::new(Expr::Eq("city".into(), ExprValue::String("Accra".into()))),
                Box::new(Expr::Eq("city".into(), ExprValue::String("Lagos".into()))),
            )
        );
    }

    #[test]
    fn parse_and_or_precedence() {
        let expr = parse_where(r#"a = "x" OR b = "y" AND c = "z""#).unwrap();
        assert_eq!(
            expr,
            Expr::Or(
                Box::new(Expr::Eq("a".into(), ExprValue::String("x".into()))),
                Box::new(Expr::And(
                    Box::new(Expr::Eq("b".into(), ExprValue::String("y".into()))),
                    Box::new(Expr::Eq("c".into(), ExprValue::String("z".into()))),
                )),
            )
        );
    }

    #[test]
    fn parse_multiple_and() {
        let expr = parse_where(r#"a = "x" AND b = "y" AND c = "z""#).unwrap();
        assert_eq!(
            expr,
            Expr::And(
                Box::new(Expr::And(
                    Box::new(Expr::Eq("a".into(), ExprValue::String("x".into()))),
                    Box::new(Expr::Eq("b".into(), ExprValue::String("y".into()))),
                )),
                Box::new(Expr::Eq("c".into(), ExprValue::String("z".into()))),
            )
        );
    }

    #[test]
    fn parse_case_insensitive_keywords() {
        let expr = parse_where(r#"city = "Accra" and age >= 25"#).unwrap();
        assert_eq!(
            expr,
            Expr::And(
                Box::new(Expr::Eq("city".into(), ExprValue::String("Accra".into()))),
                Box::new(Expr::Gte("age".into(), 25.0)),
            )
        );
    }

    #[test]
    fn parse_contains_case_insensitive() {
        let expr = parse_where(r#"tags contains "rust""#).unwrap();
        assert_eq!(
            expr,
            Expr::Contains("tags".into(), ExprValue::String("rust".into()))
        );
    }

    #[test]
    fn error_unexpected_end() {
        let err = parse_where("city =").unwrap_err();
        assert_eq!(err, ExprError::UnexpectedEnd);
    }

    #[test]
    fn error_unterminated_string() {
        let err = parse_where(r#"city = "Accra"#).unwrap_err();
        assert_eq!(err, ExprError::UnterminatedString);
    }

    #[test]
    fn error_empty_input() {
        let err = parse_where("").unwrap_err();
        assert_eq!(err, ExprError::UnexpectedEnd);
    }

    #[test]
    fn error_malformed_expression() {
        let err = parse_where(r#"= "Accra""#).unwrap_err();
        assert!(matches!(err, ExprError::UnexpectedToken(_)));
    }
}
