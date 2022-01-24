use std::io;
use std::io::{Cursor, Read, Write};
use std::process;
use crate::PrepareResult::{PREPARE_SUCCESS, PREPARE_SYNTAX_ERROR, PREPARE_UNRECOGNIZED_STATEMENT};

#[derive(PartialEq)]
pub enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}

#[derive(PartialEq)]
pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
}

#[derive(PartialEq)]
pub enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNSUPPORTED
}

pub struct Row {
    id: u32,
    username: String,
    email: String
}

pub struct Statement {
    stmt_type: StatementType,
    row_to_insert: Option<Row>
}

pub struct Table {
    u32: num_rows,
    pages: Vec<Row>
}

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = 0;
const EMAIL_OFFSET: usize = 0;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = TABLE_MAX_PAGES * ROWS_PER_PAGE;

fn main() {
    fn print_prompt() {
        print!("db > ");
    }

    fn read_input() -> String {
        let mut input_buffer = String::new();
        let bytes_read =  io::stdin()
            .read_line(&mut input_buffer)
            .expect("Failed to read line");
        if bytes_read < 0 {
            panic!("Error reading input")
        }
        String::from(input_buffer.trim())
    }

    fn do_meta_command(command: &str) -> MetaCommandResult {
        if command.eq(".exit") {
            process::exit(0x0100);
        }
        MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND
    }

    fn serialize_row(row: &Row) -> Box<Vec<u8>> {
        let mut buf = vec![];
        buf.write(row.id.to_ne_bytes().as_slice());
        write_attribute(&mut buf, row.username.as_str(), USERNAME_SIZE);
        write_attribute(&mut buf, row.email.as_str(), EMAIL_SIZE);
        Box::new(buf)
    }

    fn write_attribute(writer: &mut dyn Write, attr: &str, len: usize) {
        let attr_bytes = attr.as_bytes();
        writer.write(attr_bytes);
        writer.write(vec![0; len - attr_bytes.len()].as_slice());
    }

    fn deserialize_row(buf: &Vec<u8>) -> Box<Row>{
        let mut reader = Cursor::new(buf);
        let mut id_bytes = [0; ID_SIZE];
        reader.read_exact(&mut id_bytes);
        let mut username_bytes = [0; USERNAME_SIZE];
        reader.read_exact(&mut username_bytes);
        let mut email_bytes = [0; EMAIL_SIZE];
        reader.read_exact(&mut email_bytes);
        Box::new(Row {
            id: u32::from_ne_bytes(id_bytes),
            username: String::from_utf8(Vec::from(username_bytes)).unwrap(),
            email: String::from_utf8(Vec::from(email_bytes)).unwrap()
        })
    }

    fn prepare_statement(command: &str) -> (Box<Option<Statement>>, PrepareResult) {
        let mut stmt = if command.starts_with("insert") {
            let splits: Vec<&str> = command.split(" ").collect();
            if splits.len() < 4 {
               return (Box::new(None), PREPARE_SYNTAX_ERROR)
            }
            Statement {
                stmt_type: StatementType::STATEMENT_INSERT,
                row_to_insert: Some(Row {
                    id: splits[1].trim().parse().unwrap(),
                    username: String::from(splits[2].trim()),
                    email: String::from(splits[3].trim())
                })
            }
        } else if command.starts_with("select") {
            Statement {
                stmt_type: StatementType::STATEMENT_SELECT,
                row_to_insert: None
            }
        } else {
            Statement {
                stmt_type: StatementType::STATEMENT_UNSUPPORTED,
                row_to_insert: None
            }
        };

        if stmt.stmt_type == StatementType::STATEMENT_UNSUPPORTED {
            (Box::new(Some(stmt)), PREPARE_UNRECOGNIZED_STATEMENT)
        } else {
            (Box::new(Some(stmt)), PREPARE_SUCCESS)
        }
    }

    fn execute_statement(statement: Box<Option<Statement>>) {
        let stmt = statement.unwrap();
        match &stmt.stmt_type {
            StatementType::STATEMENT_INSERT => {
                println!("This is where we would do an insert")
            },
            StatementType::STATEMENT_SELECT => {
                println!("This is where we would do an select")
            },
            _ => println!("error")
        }
    }

    loop {
        print_prompt();
        let command = read_input();
        if command.starts_with(".") {
            let meta_result = do_meta_command(&command);
            match meta_result {
                MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND => {
                    println!("Unrecognized command {}", command);
                    continue;
                },
                MetaCommandResult::META_COMMAND_SUCCESS => continue
            }
        }

        let (stmt, prepare_result) = prepare_statement(&command);
        if prepare_result == PREPARE_UNRECOGNIZED_STATEMENT {
            println!("Unrecognized keyword at start of {}.", command);
            continue;
        }
        execute_statement(stmt);
        println!("Executed.");
    }
}
