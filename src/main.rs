use std::io;
use std::io::{Cursor, Read, Write};
use std::process;
use crate::ExecuteResult::{EXECUTE_FAIL, EXECUTE_SUCCESS, EXECUTE_TABLE_FULL};
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
pub enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_FAIL,
    EXECUTE_TABLE_FULL
}

#[derive(PartialEq)]
pub enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNSUPPORTED
}

pub struct Statement {
    stmt_type: StatementType,
    row_to_insert: Option<Row>
}

#[derive(Clone)]
pub struct Row {
    id: u32,
    username: String,
    email: String
}

impl Row {

    fn new() -> Self {
        Row {
            id: 0,
            username: String::new(),
            email: String::new()
        }
    }
}

struct Page {
    rows: Vec<Row>
}

impl Page {

    unsafe fn row_slot(&self, index: usize) -> *const Row {
        self.rows.().offset(index as isize)
    }

    unsafe fn row_mut_slot(&mut self, index: usize) -> *mut Row {
        self.rows.as_mut_ptr().offset(index as isize)
    }
}

pub struct Table {
    num_rows: usize,
    pages: Vec<Page>
}

impl Table {

    fn new() -> Self {
        Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES)
        }
    }

    unsafe fn page_slot(&mut self, index: usize) -> *const Page {
        self.pages.as_ptr().offset(index as isize)
    }

    unsafe fn page_mut_slot(&mut self, index: usize) -> *mut Page {
        self.pages.as_mut_ptr().offset(index as isize)
    }

    fn free(&mut self) {
        // TODO
    }
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

    fn row_slot(table: &mut Table, row_num: usize) -> Option<&Row> {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;
        unsafe {
            let mut page = table.page_mut_slot(page_num);
            page.row_mut_slot(row_offset);
            Some()
        }
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

    fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
        match statement.row_to_insert.as_ref() {
            Some(row) => {
                if table.num_rows > TABLE_MAX_ROWS {
                    return EXECUTE_TABLE_FULL;
                }
                let buf = serialize_row(row);
                table.num_rows += 1;
                EXECUTE_SUCCESS
            },
            None => EXECUTE_FAIL
        }
    }

    fn execute_select(statement: &Statement, table: &Table) -> ExecuteResult {
        for i in 0..table.num_rows {
            let page = &table.pages[i];
            for j in 0..page.len() {
                deserialize_row(&page[j])
            }
        }
        EXECUTE_SUCCESS
    }

    fn execute_statement(statement: Box<Option<Statement>>, table: &mut Table) -> ExecuteResult {
        let stmt = statement.unwrap();
        match &stmt.stmt_type {
            StatementType::STATEMENT_INSERT => execute_insert(&stmt, table),
            StatementType::STATEMENT_SELECT => execute_select(&stmt, table),
            _ => ExecuteResult::EXECUTE_FAIL
        }
    }

    let mut table = Table::new();
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
        match prepare_result {
            PREPARE_UNRECOGNIZED_STATEMENT => {
                println!("Unrecognized keyword at start of {}.", command);
                continue;
            },
            PREPARE_SYNTAX_ERROR => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            _ => {}
        }
        execute_statement(stmt, &mut table);
        println!("Executed.");
    }
}
