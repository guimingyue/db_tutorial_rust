use std::fs::{File, OpenOptions};
use std::{env, io};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::process;
use crate::ExecuteResult::{EXECUTE_FAIL, EXECUTE_SUCCESS, EXECUTE_TABLE_FULL};
use crate::PrepareResult::{PREPARE_NEGATIVE_ID, PREPARE_STRING_TOO_LONG, PREPARE_SUCCESS, PREPARE_SYNTAX_ERROR, PREPARE_UNRECOGNIZED_STATEMENT};

#[derive(PartialEq)]
pub enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}

#[derive(PartialEq)]
pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID
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

struct Page {
    rows: Vec<Row>
}

impl Page {

    fn new() -> Self {
        Page {
            rows: Vec::with_capacity(ROWS_PER_PAGE)
        }
    }

    unsafe fn row_slot(&self, index: usize) -> *const Row {
        self.rows.as_ptr().offset(index as isize)
    }

    unsafe fn row_mut_slot(&mut self, index: usize) -> *mut Row {
        if self.rows.capacity() <= 0 {
            self.rows.reserve(ROWS_PER_PAGE);
        }
        self.rows.as_mut_ptr().offset(index as isize)
    }

    unsafe fn serialize(&self, buf: &mut Vec<u8>) {
        fn write_attribute(writer: &mut dyn Write, attr: &str, len: usize) {
            let attr_bytes = attr.as_bytes();
            writer.write(attr_bytes);
            writer.write(vec![0; len - attr_bytes.len()].as_slice());
        }

        for i in 0..self.rows.len() {
            let row = self.row_slot(i);
            buf.write((*row).id.to_ne_bytes().as_slice());
            write_attribute(buf, (*row).username.as_str(), USERNAME_SIZE);
            write_attribute(buf, (*row).email.as_str(), EMAIL_SIZE);
        }
    }

    fn load(&mut self, bytes: &[u8]) {
        let mut idx = 0;
        while idx + ROW_SIZE < bytes.len() {
            let mut reader = Cursor::new(&bytes[idx..idx + ROW_SIZE]);
            let mut id_bytes = [0; ID_SIZE];
            reader.read_exact(&mut id_bytes);
            let mut username_bytes = [0; USERNAME_SIZE];
            reader.read_exact(&mut username_bytes);
            let mut email_bytes = [0; EMAIL_SIZE];
            reader.read_exact(&mut email_bytes);
            self.rows.push(Row {
                id: u32::from_ne_bytes(id_bytes),
                username: String::from_utf8(Vec::from(username_bytes)).unwrap(),
                email: String::from_utf8(Vec::from(email_bytes)).unwrap()
            });
            idx += ROW_SIZE;
        }
    }
}

pub struct Pager {
    file_descriptor: File,
    pages: Vec<Page>
}

impl Pager {

    fn new(file: File) -> Self {
        Pager {
            file_descriptor: file,
            pages: Vec::with_capacity(TABLE_MAX_PAGES)
        }
    }

    fn file_length(&self) -> u64 {
        self.file_descriptor.metadata().unwrap().len()
    }

    fn num_pages(&self) -> usize {
        let mut num_page = self.file_length() / PAGE_SIZE as u64;
        if self.file_length() % PAGE_SIZE as u64 != 0 {
            num_page += 1;
        }
        num_page as usize
    }

    unsafe fn get_page(&mut self, page_num: usize) -> *mut Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }
        let mut page = self.page_mut_slot(page_num);
        if (*page).rows.capacity() == 0 {
            // allocate page memory
            let mut new_page = Page::new();
            if page_num <= self.num_pages() {
                self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
                let mut buf = [0; PAGE_SIZE];
                let result = self.file_descriptor.read(&mut buf);
                if let Err(errno) = result {
                    println!("Error reading file: {}", errno);
                    process::exit(0x0100);
                }
                new_page.load(&buf);
                std::ptr::write(page, new_page);
            }
        }
        page
    }

    pub fn flush_page(&mut self, page_num: usize) {
        unsafe {
            let page = self.page_slot(page_num);
            if page.is_null() {
                return;
            }
            self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
            let mut buf = vec![];
            (*page).serialize(&mut buf);
            self.file_descriptor.write(buf.as_slice());
            self.file_descriptor.flush();
        }
    }

    unsafe fn page_slot(&self, index: usize) -> *const Page {
        self.pages.as_ptr().offset(index as isize)
    }

    unsafe fn page_mut_slot(&mut self, index: usize) -> *mut Page {
        self.pages.as_mut_ptr().offset(index as isize)
    }


    fn close(&mut self) {
        self.file_descriptor.flush();
    }
}

pub struct Table {
    num_rows: usize,
    pager: Pager
}

impl Table {

    fn new(pager: Pager) -> Self {
        let file_length = pager.file_length();
        Table {
            pager,
            num_rows: file_length as usize / ROW_SIZE
        }
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

    fn do_meta_command(command: &str, table: &mut Table) -> MetaCommandResult {
        if command.eq(".exit") {
            db_close(table);
            process::exit(0x0100);
        }
        MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND
    }

    fn pager_open(file_name: &str) -> Pager {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_name)
            .unwrap();

        Pager::new(file)
    }

    fn db_open(file_name: &str) -> Table {
        let pager = pager_open(file_name);
        Table::new(pager)
    }

    fn db_close(table: &mut Table) {
        let num_full_pages = table.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            table.pager.flush_page(i);
        }
        let num_additional_rows = table.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            table.pager.flush_page(num_full_pages);
        }
    }

    unsafe fn row_mut_slot(table: &mut Table, row_num: usize) -> *mut Row {
        let page = table.pager.get_page(row_num / ROWS_PER_PAGE);
        (*page).row_mut_slot(row_num % ROWS_PER_PAGE)
    }

    unsafe fn row_slot(table: &mut Table, row_num: usize) -> *const Row {
        let page = table.pager.get_page(row_num / ROWS_PER_PAGE);
        if page.is_null() {
            return std::ptr::null();
        }
        (*page).row_slot(row_num % ROWS_PER_PAGE)
    }

    fn prepare_insert(command: &str) -> Result<Box<Option<Statement>>, PrepareResult> {
        let splits: Vec<&str> = command.split(" ").collect();
        if splits.len() < 4 {
            return Err(PREPARE_SYNTAX_ERROR);
        }
        let id = splits[1].trim().parse().unwrap();
        if id < 0 {
            return Err(PREPARE_NEGATIVE_ID);
        }
        let username = splits[2].trim();
        if username.len() > USERNAME_SIZE {
            return Err(PREPARE_STRING_TOO_LONG);
        }
        let mut username_vec = vec![0; USERNAME_SIZE];
        unsafe {
            std::ptr::copy(username.as_ptr(), username_vec.as_mut_ptr(), username.len());
        }

        let email = splits[3].trim();
        if email.len() > EMAIL_SIZE {
            return Err(PREPARE_STRING_TOO_LONG);
        }
        let mut email_vec = vec![0; EMAIL_SIZE];
        unsafe {
            std::ptr::copy(email.as_ptr(), email_vec.as_mut_ptr(), email.len());
        }
        Ok(Box::new(Some(Statement {
            stmt_type: StatementType::STATEMENT_INSERT,
            row_to_insert: Some(Row {
                id,
                username: String::from_utf8(username_vec).unwrap(),
                email: String::from_utf8(email_vec).unwrap()
            })
        })))
    }

    fn prepare_statement(command: &str) -> Result<Box<Option<Statement>>, PrepareResult> {
        if command.starts_with("insert") {
            prepare_insert(command)
        } else if command.starts_with("select") {
            Ok(Box::new(Some(Statement {
                stmt_type: StatementType::STATEMENT_SELECT,
                row_to_insert: None
            })))
        } else {
            Err(PREPARE_UNRECOGNIZED_STATEMENT)
        }
    }

    fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
        match statement.row_to_insert.as_ref() {
            Some(row_to_insert) => {
                if table.num_rows > TABLE_MAX_ROWS {
                    return EXECUTE_TABLE_FULL;
                }

                unsafe {
                    let row = row_mut_slot(table, table.num_rows);
                    std::ptr::write(row, Row {
                        id: (*row_to_insert).id,
                        username: String::from((*row_to_insert).username.as_str()),
                        email: String::from((*row_to_insert).email.as_str())
                    });
                }
                table.num_rows += 1;
                EXECUTE_SUCCESS
            },
            None => EXECUTE_FAIL
        }
    }

    fn execute_select(statement: &Statement, table: &mut Table) -> ExecuteResult {
        for i in 0..table.num_rows {
            unsafe {
                let row = row_slot(table, i);
                println!("{}, {}, {}", (*row).id, (*row).username, (*row).email)
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

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Must supply a database filename.");
        process::exit(0x0100);
    }
    let mut table = db_open(args[1].as_str());
    loop {
        print_prompt();
        let command = read_input();
        if command.starts_with(".") {
            let meta_result = do_meta_command(&command, &mut table);
            match meta_result {
                MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND => {
                    println!("Unrecognized command {}", command);
                    continue;
                },
                MetaCommandResult::META_COMMAND_SUCCESS => continue
            }
        }

        match prepare_statement(&command) {
            Ok(stmt) => execute_statement(stmt, &mut table),
            Err(prepare_result) => {
                match prepare_result {
                    PREPARE_UNRECOGNIZED_STATEMENT =>
                        println!("Unrecognized keyword at start of {}.", command),
                    PREPARE_SYNTAX_ERROR =>
                        println!("Syntax error. Could not parse statement."),
                    PREPARE_STRING_TOO_LONG =>
                        println!("String is too long."),
                    PREPARE_NEGATIVE_ID =>
                        println!("ID must be positive."),
                    _ => {},
                };
                continue;
            }

        };
        println!("Executed.");
    }
}
