use std::fs::{File, OpenOptions};
use std::{env, io};
use std::io::{Read, Seek, SeekFrom, Write};
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

pub enum NodeType {
    NODE_INTERNAL,
    NODE_LEAF
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

pub struct Page {
    buf: [u8; PAGE_SIZE]
}

impl Page {

    fn new() -> Self {
        Page {
            buf: [0; PAGE_SIZE]
        }
    }

    unsafe fn row_slot(&self, index: usize) -> *const Row {
        // self.rows.as_ptr().offset(index as isize)
        std::ptr::null()
    }

    unsafe fn row_mut_slot(&mut self, index: usize) -> *mut Row {
        /*if self.rows.capacity() <= 0 {
            self.rows.reserve(ROWS_PER_PAGE);
        }
        self.rows.as_mut_ptr().offset(index as isize)*/
        std::ptr::null_mut()
    }

    fn load(&mut self, bytes: &[u8]) {
        fn read_end_idx(bytes: &[u8]) -> usize {
            for i in (0..bytes.len()).rev() {
                if bytes[i] != 0 {
                    return i;
                }
            }
            0
        }
        let mut idx = 0;
        while idx + ROW_SIZE <= bytes.len() {
            let mut reader = std::io::Cursor::new(&bytes[idx..idx + ROW_SIZE]);
            let mut id_bytes = [0; ID_SIZE];
            reader.read_exact(&mut id_bytes);
            let mut username_bytes = [0; USERNAME_SIZE];
            reader.read_exact(&mut username_bytes);
            let mut email_bytes = [0; EMAIL_SIZE];
            reader.read_exact(&mut email_bytes);
            /*self.rows.push(Row {
                id: u32::from_ne_bytes(id_bytes),
                username: String::from_utf8(Vec::from(&username_bytes[0..=read_end_idx(&username_bytes)])).unwrap(),
                email: String::from_utf8(Vec::from(&email_bytes[0..=read_end_idx(&email_bytes)])).unwrap()
            });*/
            idx += ROW_SIZE;
        }
    }

    pub fn leaf_node_num_cells(&self) -> *mut usize {
        self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize
    }

    fn index(&self, offset: usize) -> u8 {
        unsafe {
            (self.buf.as_ptr() as u8).checked_add(offset as u8).unwrap()
        }
    }

    fn leaf_node_cell(&self, cell_num: usize) -> * const usize {
        (self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE)) as *const usize
    }

    fn leaf_node_key(&self, cell_num: usize) -> *mut u32 {
        self.leaf_node_cell(cell_num) as *mut u32
    }

    fn leaf_node_value(&self, cell_num: usize) -> *mut Row {
        self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE) as *mut Row
    }

    fn initialize_leaf_node(&mut self) {
        unsafe {
            *(self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize) = 0;
        }
    }

    fn is_full(&self) -> bool {
        unsafe {
            (*self.leaf_node_num_cells()) >= LEAF_NODE_MAX_CELLS
        }
    }

    fn is_leaf_node(&self) -> bool {
        // TODO
        true
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
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        }
    }

    fn file_length(&self) -> u64 {
        self.file_descriptor.metadata().unwrap().len()
    }

    fn num_pages(&self) -> usize {
        let mut num_page = self.file_length() / PAGE_SIZE as u64;
        if self.file_length() % PAGE_SIZE as u64 != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            process::exit(0x0100);
        }
        num_page as usize
    }

    unsafe fn get_page(&mut self, page_num: usize) -> *mut Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }
        let mut page = self.page_mut_slot(page_num);
        if page.is_null() {
            // allocate page memory
            if page_num <= self.num_pages() {
                let mut new_page = Page::new();
                self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
                let result = self.file_descriptor.read(&mut new_page.buf);
                match result {
                    Ok(len) => {
                        // new_page.load(&buf[0..len]);
                        std::ptr::write(page, new_page);
                    },
                    Err(errno) => {
                        println!("Error reading file: {}", errno);
                        process::exit(0x0100);
                    }
                };
            }
        }
        page
    }

    pub fn pager_flush(&mut self, page_num: usize) {
        unsafe {
            let page = self.page_slot(page_num);
            self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
            self.file_descriptor.write((*page).buf.as_slice());
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
    root_page_num: usize,
    pager: Pager
}

impl Table {

    fn new(pager: Pager) -> Self {
        let file_length = pager.file_length();
        Table {
            pager,
            root_page_num: 0
        }
    }
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: usize,
    end_of_table: bool
}

impl <'a> Cursor<'a> {

    pub fn table_start(table: &'a mut Table) -> Self {
        let root_page_num = table.root_page_num;

        let root_node = unsafe { table.pager.get_page(root_page_num) };
        let num_cells = unsafe { *((*root_node).leaf_node_num_cells()) };

        Cursor {
            table,
            cell_num: 0,
            page_num: root_page_num,
            end_of_table: num_cells == 0
        }
    }

    pub fn table_end(table: &'a mut Table) -> Self {
        let root_node = unsafe { table.pager.get_page(table.root_page_num) };
        let num_cells = unsafe { *((*root_node).leaf_node_num_cells()) };
        Cursor {
            page_num: table.root_page_num,
            cell_num: num_cells,
            end_of_table: true,
            table
        }
    }

    pub fn get_page(&mut self) -> *mut Page{
        unsafe {
            self.table.pager.get_page(self.page_num)
        }
    }

    pub fn advance(&mut self) {
        unsafe {
            let page = self.table.pager.get_page(self.page_num);
            self.cell_num += 1;
            if self.cell_num >= *((*page).leaf_node_num_cells()) {
                self.end_of_table = true;
            }
        }
    }

    pub fn cursor_mut_value(&mut self) -> *mut Row{
        let page = self.get_page();
        unsafe {
            (*page).leaf_node_value(self.cell_num)
        }
    }

    pub fn cursor_value(&mut self) -> *const Row{
        let page = self.get_page();
        unsafe {
            (*page).row_mut_slot(self.cell_num)
        }
    }

    pub unsafe fn leaf_node_insert(&mut self, key: u32, value: &Row) {
        let page = self.get_page();
        let num_cells = (*page).leaf_node_num_cells();
        if *num_cells > LEAF_NODE_MAX_CELLS {
            println!("Need to implement splitting a leaf node.");
            // TODO
            process::exit(-1);
        }
        if self.cell_num < *num_cells {
            // shift cell from cell_num to num_cells to right to make room for new cell
            // TODO
        }
        (*num_cells) += 1;
        *((*page).leaf_node_key(self.cell_num)) = key;

        let row = (*page).leaf_node_value(self.cell_num);
        let src_row_ptr = value as *const Row;
        std::ptr::copy(src_row_ptr as *const u8, row as *mut u8, ID_SIZE);
        std::ptr::copy((src_row_ptr as u8 + USERNAME_OFFSET as u8) as *const u8, (row as u8 + USERNAME_OFFSET as u8) as *mut u8, USERNAME_SIZE);
        std::ptr::copy((src_row_ptr  as u8 + EMAIL_OFFSET  as u8) as *const u8, (row as u8 + EMAIL_OFFSET as u8) as *mut u8, EMAIL_SIZE);
    }


}

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = TABLE_MAX_PAGES * ROWS_PER_PAGE;

/// Common Node Header Layout:
/// NODE TYPE|IS ROOT|PARENT POINTER
const NODE_TYPE_SIZE: usize = std::mem::size_of::<NodeType>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = std::mem::size_of::<bool>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<usize>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_SIZE + IS_ROOT_OFFSET;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/// Leaf Node Header Layout:
/// Common Node Header|Cell num of Leaf Node
const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<usize>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/// Leaf Node Body Layout:
/// [Leaf Node Key|Leaf Node Value]
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<usize>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

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
        // todo return Box<Pager>
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_name)
            .unwrap();

        let mut pager = Pager::new(file);
        if pager.num_pages() == 0 {
            unsafe {
                let mut root_node = pager.get_page(0);
                (*root_node).initialize_leaf_node();
            }
        }
        pager
    }

    fn db_open(file_name: &str) -> Table {
        let pager = pager_open(file_name);
        Table::new(pager)
    }

    fn db_close(table: &mut Table) {
        for i in 0..table.pager.num_pages() {
            table.pager.pager_flush(i);
        }
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

        let email = splits[3].trim();
        if email.len() > EMAIL_SIZE {
            return Err(PREPARE_STRING_TOO_LONG);
        }
        Ok(Box::new(Some(Statement {
            stmt_type: StatementType::STATEMENT_INSERT,
            row_to_insert: Some(Row {
                id,
                username: String::from(username),
                email: String::from(email)
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
                unsafe {
                    let page = table.pager.get_page(table.root_page_num);
                    if (*page).is_full() {
                        return EXECUTE_TABLE_FULL;
                    }
                }
                let mut cursor = Cursor::table_end(table);
                unsafe { cursor.leaf_node_insert((*row_to_insert).id, row_to_insert) };
                EXECUTE_SUCCESS
            },
            None => EXECUTE_FAIL
        }
    }

    fn execute_select(statement: &Statement, table: &mut Table) -> ExecuteResult {
        let mut cursor = Cursor::table_start(table);
        while !cursor.end_of_table {
            let row = cursor.cursor_value();
            unsafe {
                println!("{}, {}, {}", (*row).id, (*row).username, (*row).email)
            }
            cursor.advance();
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
