use std::io;
use std::process;
use crate::PrepareResult::{PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT};

#[derive(PartialEq)]
pub enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}

#[derive(PartialEq)]
pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
}

#[derive(PartialEq)]
pub enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNSUPPORTED
}

pub struct Statement {
    stmt_type: StatementType
}

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

    fn prepare_statement(command: &str) -> (Box<Statement>, PrepareResult) {
        let stmt_type= if command.starts_with("insert") {
            StatementType::STATEMENT_INSERT
        } else if command.starts_with("select") {
            StatementType::STATEMENT_SELECT
        } else {
            StatementType::STATEMENT_UNSUPPORTED
        };

        let stmt = Statement {
            stmt_type
        };

        if stmt.stmt_type == StatementType::STATEMENT_UNSUPPORTED {
            (Box::new(stmt), PREPARE_UNRECOGNIZED_STATEMENT)
        } else {
            (Box::new(stmt), PREPARE_SUCCESS)
        }
    }

    fn execute_statement(stmt: &Statement) {
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
        execute_statement(&stmt);
        println!("Executed.");
    }
}
