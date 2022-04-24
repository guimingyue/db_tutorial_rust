use std::fs::{File, OpenOptions};
use std::{env, io};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::Rev;
use std::ops::Range;
use std::process;
use std::thread::current;
use crate::ExecuteResult::{EXECUTE_DUPLICATE_KEY, EXECUTE_FAIL, EXECUTE_SUCCESS, EXECUTE_TABLE_FULL};
use crate::NodeType::{NODE_INTERNAL, NODE_LEAF};
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
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY
}

#[derive(PartialEq)]
pub enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNSUPPORTED
}

#[derive(PartialEq)]
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

    unsafe fn row_mut_slot(&self, cell_num: usize) -> Box<Row> {
        fn read_end_idx(bytes: &[u8]) -> usize {
            for i in (0..bytes.len()).rev() {
                if bytes[i] != 0 {
                    return i;
                }
            }
            0
        }
        let cell = self.leaf_node_value(cell_num);

        let id = std::ptr::read(cell as *const u32);
        let username_bytes = std::ptr::read((cell as usize + USERNAME_OFFSET) as *const [u8; USERNAME_SIZE]);
        let email_bytes = std::ptr::read((cell as usize + EMAIL_OFFSET) as *const [u8; EMAIL_SIZE]);

        Box::new(Row {
            id,
            username: String::from_utf8_unchecked(Vec::from(&username_bytes[0..=read_end_idx(&username_bytes)])),
            email: String::from_utf8_unchecked(Vec::from(&email_bytes[0..=read_end_idx(&email_bytes)]))
        })
    }

    fn load(&mut self, bytes: &[u8]) {
        let mut idx = 0;
        while idx + ROW_SIZE <= bytes.len() {
            let mut reader = std::io::Cursor::new(&bytes[idx..idx + ROW_SIZE]);
            let mut id_bytes = [0; ID_SIZE];
            reader.read_exact(&mut id_bytes);
            let mut username_bytes = [0; USERNAME_SIZE];
            reader.read_exact(&mut username_bytes);
            let mut email_bytes = [0; EMAIL_SIZE];
            reader.read_exact(&mut email_bytes);
            idx += ROW_SIZE;
        }
    }

    unsafe fn leaf_node_mut_num_cells(&self) -> *mut usize {
        self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize
    }

    fn leaf_node_num_cells(&self) -> usize {
        unsafe {*self.leaf_node_mut_num_cells()}
    }

    fn set_leaf_node_num_cells(&mut self, num_cells: usize) {
        unsafe {
            *self.leaf_node_mut_num_cells() = num_cells
        }
    }

    fn index(&self, offset: usize) -> isize {
        let ptr = self.buf.as_ptr();
        unsafe {
            (ptr as isize).checked_add(offset as isize).unwrap()
        }
    }

    fn leaf_node_cell(&self, cell_num: usize) -> *const u8 {
        (self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE)) as *const u8
    }

    fn leaf_node_key(&self, cell_num: usize) -> u32 {
        unsafe { *(self.leaf_node_cell(cell_num) as *mut u32) }
    }

    fn set_leaf_node_key(&self, cell_num: usize, key: u32) {
        unsafe { *(self.leaf_node_cell(cell_num) as *mut u32) = key }
    }

    fn leaf_node_value(&self, cell_num: usize) -> *mut u8 {
        self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE) as *mut u8
    }

    fn initialize_leaf_node(&mut self) {
        self.set_node_type(NODE_LEAF);
        self.set_node_root(false);
        let ptr = self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize;
        unsafe {
            *ptr = 0;
        }
    }

    fn initialize_internal_node(&mut self) {
        self.set_node_type(NODE_INTERNAL);
        self.set_node_root(false);
        let ptr = self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize;
        unsafe {
            *ptr = 0;
        }
    }

    fn is_full(&self) -> bool {
        self.leaf_node_num_cells() >= LEAF_NODE_MAX_CELLS
    }

    fn is_leaf_node(&self) -> bool {
        // TODO
        true
    }

    fn get_node_type<'a>(&self) -> &'a NodeType {
        unsafe { &*(self.index(NODE_TYPE_OFFSET) as *const NodeType) }
    }

    fn set_node_type(&mut self, node_type: NodeType) {
        let ptr = self.index(NODE_TYPE_OFFSET) as *mut u8;
        unsafe {
            *ptr = node_type as u8;
        }
    }

    pub fn is_node_root(&self) -> bool {
        unsafe { *(self.index(IS_ROOT_OFFSET) as *const bool) }
    }

    pub fn set_node_root(&mut self, is_root: bool) {
        unsafe {
            *(self.index(IS_ROOT_OFFSET) as *mut bool) = is_root;
        }
    }

    fn internal_node_right_child(&self) -> isize {
        self.index(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
    }

    pub fn set_internal_node_right_child(&mut self, internal_node_right_child: usize) {
        unsafe {
            *(self.internal_node_right_child() as *mut usize) = internal_node_right_child;
        }
    }

    pub fn get_internal_node_right_child(&self) -> usize {
        unsafe {
            *(self.internal_node_right_child() as *mut usize)
        }
    }

    pub fn set_internal_node_num_keys(&mut self, num_keys: usize) {
        unsafe {
            *(self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize) = num_keys;
        }
    }

    pub fn get_internal_node_num_keys(&self) -> usize {
        unsafe {
            *(self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut usize)
        }
    }

    pub fn internal_node_cell(&self, cell_num: usize) -> isize {
        self.index(INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE)
    }

    fn set_internal_node_cell(&mut self, cell_num: usize, page_num: usize) {
        unsafe { *(self.internal_node_cell(cell_num) as *mut usize) = page_num }
    }

    fn get_internal_node_cell(&self, cell_num: usize) -> usize {
        unsafe { *(self.internal_node_cell(cell_num) as *const usize) }
    }

    pub fn set_internal_node_child(&mut self, child_num: usize, child_page_num: usize) {
        let num_keys = self.get_internal_node_num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {} > num_keys {}", child_num, num_keys);
            process::exit(0x0010);
        } else if child_num == num_keys {
            self.set_internal_node_right_child(child_page_num);
        } else {
            self.set_internal_node_cell(child_num, child_page_num);
        }
    }

    pub fn get_internal_node_child(&self, child_num: usize) -> usize {
        let num_keys = self.get_internal_node_num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {}", child_num);
            process::exit(0x0010);
        } else if child_num == num_keys {
            self.get_internal_node_right_child()
        } else {
            self.get_internal_node_cell(child_num)
        }
    }

    pub fn set_internal_node_key(&mut self, key_num: usize, key_val: u32) {
        unsafe {
            *((self.internal_node_cell(key_num) + INTERNAL_NODE_CHILD_SIZE as isize) as *mut u32) = key_val;
        }
    }

    fn get_internal_node_key(&self, cell_num: usize) -> u32 {
        unsafe {
            *((self.internal_node_cell(cell_num) + INTERNAL_NODE_CHILD_SIZE as isize) as *const u32)
        }
    }

    pub fn get_node_max_key(&self) -> u32 {
        match self.get_node_type() {
            NODE_INTERNAL => self.get_internal_node_key(self.get_internal_node_num_keys() - 1),
            NODE_LEAF => self.leaf_node_key(self.leaf_node_num_cells() - 1)
        }
    }
}

pub struct Pager {
    file_descriptor: File,
    pages: Vec<Option<Box<Page>>>,
    num_pages: usize
}

impl Pager {

    fn new(file: File) -> Self {
        fn num_pages_file(file_length: u64) -> usize {
            let mut num_page = file_length / PAGE_SIZE as u64;
            if file_length % PAGE_SIZE as u64 != 0 {
                println!("Db file is not a whole number of pages. Corrupt file.");
                process::exit(0x0100);
            }
            num_page as usize
        }
        Pager {
            num_pages: num_pages_file(file.metadata().unwrap().len()),
            file_descriptor: file,
            pages: std::iter::repeat_with(|| None).take(TABLE_MAX_PAGES).collect::<Vec<_>>()
        }
    }

    fn get_page_view(&self, page_num: usize) -> Option<&Page> {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }

        match &self.pages[page_num] {
            Some(page) => Some(page.as_ref()),
            _ => None
        }
    }

    fn get_page(&mut self, page_num: usize) -> &mut Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }
        let page = &self.pages[page_num];
        if page.is_none() {
            // create a page in memory
            let mut new_page = Page::new();
            if page_num <= self.num_pages {
                self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
                let result = self.file_descriptor.read(&mut new_page.buf);
                if result.is_err() {
                    println!("Error reading file: {}", result.unwrap());
                    process::exit(0x0100);
                }
            }
            self.pages[page_num] = Some(Box::new(new_page));
            // TODO
            if page_num >= self.num_pages {
                self.num_pages += 1;
            }
        }
        let page = &mut self.pages[page_num];
        page.as_mut().unwrap()
    }

    pub fn pager_flush(&mut self, page_num: usize) {
        match &self.pages[page_num] {
            Some(page) => {
                self.file_descriptor.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
                self.file_descriptor.write(page.buf.as_slice());
                self.file_descriptor.flush();
            },
            None => ()

        }
    }

    fn close(&mut self) {
        self.file_descriptor.flush();
    }

    fn get_unused_page_num(&self) -> usize {
        self.num_pages
    }
}

pub struct Table {
    root_page_num: usize,
    pager: Pager
}

impl Table {

    fn new(pager: Pager) -> Self {
        Table {
            pager,
            root_page_num: 0
        }
    }

    fn find(&self, key: u32) -> (usize, usize) {
        let root_page_num = self.root_page_num;
        let page = self.pager.get_page_view(root_page_num);
        if page.is_none() {
            return (0, 0);
        }
        let page = page.unwrap();

        if *page.get_node_type() == NODE_LEAF {
            let num_cells = page.leaf_node_num_cells();
            let (mut min_index, mut one_past_max_index) = (0, num_cells);
            while one_past_max_index != min_index {
                let index = (one_past_max_index + min_index) / 2;
                let key_at_index = page.leaf_node_key(index);
                if key_at_index == key {
                    // return
                    return (root_page_num, index)
                } else if key_at_index > key {
                    one_past_max_index = index;
                } else {
                    min_index = index + 1;
                }
            }
            (root_page_num, min_index)
        } else {
            println!("Need to implement searching an internal node");
            process::exit(0x0010);
        }
    }

    pub fn print_tree(&self) {
        fn print_tree_node(pager: &Pager, page_num: usize, indentation_level: usize) {
            fn indent(level: usize) {
                (0..level).for_each(|i| print!(" "));
            }
            let node = pager.get_page_view(page_num);
            match node {
                Some(page) => {
                    match page.get_node_type() {
                        NodeType::NODE_LEAF => {
                            let num_keys = page.leaf_node_num_cells();
                            indent(indentation_level);
                            println!("- leaf (size {})", num_keys);
                            for i in 0..num_keys {
                                indent(indentation_level + 1);
                                println!("{}", page.leaf_node_key(i));
                            }
                        },
                        NodeType::NODE_INTERNAL => {
                            let num_keys = page.get_internal_node_num_keys();
                            indent(indentation_level);
                            println!("- internal (size {})", num_keys);
                            for i in 0..num_keys {
                                let child = page.get_internal_node_child(i);
                                print_tree_node(pager, child, indentation_level + 1);
                                indent(indentation_level + 1);
                                println!("- key {}", page.get_internal_node_key(i));
                            }
                            let child = page.get_internal_node_right_child();
                            print_tree_node(pager, child, indentation_level + 1);
                        }
                    }
                },
                _ => ()
            }

        };

        print_tree_node(&self.pager, 0, 0);
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

        let root_node = table.pager.get_page(root_page_num);
        let num_cells = root_node.leaf_node_num_cells();

        Cursor {
            table,
            cell_num: 0,
            page_num: root_page_num,
            end_of_table: num_cells == 0
        }
    }

    pub fn get_page(&mut self) -> &mut Page{
        self.table.pager.get_page(self.page_num)
    }

    pub fn get_page_view(&self) -> Option<&Page> {
        self.table.pager.get_page_view(self.page_num)
    }

    pub fn advance(&mut self) {
        let page = self.table.pager.get_page_view(self.page_num).unwrap();
        self.cell_num += 1;
        if self.cell_num >= page.leaf_node_num_cells() {
            self.end_of_table = true;
        }
    }

    pub fn cursor_value(&mut self) -> Box<Row> {
        let cell_num = self.cell_num;
        let page = self.get_page_view().unwrap();
        unsafe { page.row_mut_slot(cell_num) }
    }

    pub unsafe fn leaf_node_insert(&mut self, key: u32, value: &Row) {
        let cell_num = self.cell_num;
        let page = self.get_page();
        let num_cells = page.leaf_node_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS {
            self.leaf_node_split_and_insert(value.id, value);
            return;
        }
        if cell_num < num_cells {
            // shift cell from cell_num to num_cells to right to make room for new cell
            for i in (cell_num + 1..=num_cells).rev() {
                std::ptr::copy_nonoverlapping(page.leaf_node_cell(i - 1),
                                              page.leaf_node_cell(i) as *mut u8,
                                              LEAF_NODE_CELL_SIZE);
            }
        }
        page.set_leaf_node_num_cells(num_cells + 1);
        page.set_leaf_node_key(cell_num, key);

        let cell = page.leaf_node_value(cell_num);
        serialize_row(cell, value);
    }

    /// Create a new node and move half the cells over.
    ///
    /// Insert the new value in one of the two nodes.
    ///
    /// Update parent or create a new parent.
    ///
    /// The implementation of this method is different from the origin c code which can be found in
    /// [Part 10 - Splitting a Leaf Node](https://cstack.github.io/db_tutorial/parts/part10.html#splitting-algorithm).
    /// Because of the reference borrow checker mechanism of Rust，only one mutable reference can be
    /// borrowed at one time, so the copy page data process should be splitted into two code block.
    fn leaf_node_split_and_insert(&mut self, key: u32, value: &Row) {
        // create a new right node
        let value_cell_num = self.cell_num;
        // page that will be created
        let new_page_num = self.table.pager.get_unused_page_num();
        {
            let old_node = self.get_page_view().unwrap();
            let old_node_ptr = old_node as *const Page;
            let new_node = self.table.pager.get_page(new_page_num);
            // init and copy cells to new right node from old node
            new_node.initialize_leaf_node();
            copy_page_data((LEAF_NODE_LEFT_SPLIT_COUNT..LEAF_NODE_MAX_CELLS + 1).rev(), old_node_ptr, new_node, value, value_cell_num);
            new_node.set_leaf_node_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
        }

        let mut is_node_root = false;
        {
            // Move cell that still in old node to new position.
            // for example, the node [1, 3, 5, 7, 9] is full, and cell 2 is being inserted now,
            // so we should split this node, and [5, 7, 9] is the new node. At the same time,
            // cell 3 should be moved to the next space, after the, cell 2 can be inserted into
            // the old node. So the old node is [1, 2, 3] after inserting is finished.
            let old_node = self.get_page();
            is_node_root = old_node.is_node_root();
            copy_page_data((0..LEAF_NODE_LEFT_SPLIT_COUNT).rev(), old_node as *const Page, old_node, value, value_cell_num);
            old_node.set_leaf_node_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);
        }

        if is_node_root {
            self.create_new_node(new_page_num);
        } else {
            println!("Need to implement updating parent after split");
            process::exit(0x0010);
        }
    }

    fn create_new_node(&mut self, right_child_page_num: usize) {
        // create new root node
        let left_child_page_num = self.table.pager.get_unused_page_num();
        let mut node_max_key;
        {
            let old_node = self.get_page_view().unwrap();
            let old_node_ptr = old_node as *const Page;
            let left_child = self.table.pager.get_page(left_child_page_num);
            unsafe {
                std::ptr::copy(old_node_ptr as *const u8, left_child as *mut Page as *mut u8, PAGE_SIZE);
                left_child.set_node_root(false);
            }
            node_max_key = left_child.get_node_max_key();
        }

        let old_node = self.get_page();
        old_node.initialize_internal_node();
        old_node.set_node_root(true);
        old_node.set_internal_node_num_keys(1);
        old_node.set_internal_node_child(0, left_child_page_num);
        old_node.set_internal_node_key(0, node_max_key);
        old_node.set_internal_node_right_child(right_child_page_num);
    }
}

unsafe fn serialize_row(cell: *mut u8, source: &Row) {
    std::ptr::write(cell as *mut u32, source.id);

    std::ptr::write((cell as usize + USERNAME_OFFSET) as *mut [u8; USERNAME_SIZE], [0 as u8; USERNAME_SIZE]);
    std::ptr::copy(source.username.as_ptr(), (cell as usize + USERNAME_OFFSET) as *mut u8, source.username.len());

    std::ptr::write((cell as usize + EMAIL_OFFSET) as *mut [u8; EMAIL_SIZE], [0 as u8; EMAIL_SIZE]);
    std::ptr::copy(source.email.as_ptr(), (cell as usize + EMAIL_OFFSET) as *mut u8, source.email.len());
}

fn copy_page_data(rang: Rev<Range<usize>>, src_ptr: *const Page, dst_page: &mut Page, value: &Row, value_cell_num: usize) {
    for i in rang {
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        let destination = dst_page.leaf_node_cell(index_within_node);
        unsafe {
            if i == value_cell_num {
                serialize_row(destination as *mut u8, value);
            } else if i > value_cell_num {
                std::ptr::copy((*src_ptr).leaf_node_cell(i - 1), destination as *mut u8, LEAF_NODE_CELL_SIZE);
            } else {
                std::ptr::copy((*src_ptr).leaf_node_cell(i), destination as *mut u8, LEAF_NODE_CELL_SIZE)
            }
        }
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
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/// Internal Node Header Layout
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/// Internal Node Body Layout
const INTERNAL_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const INTERNAL_NODE_CHILD_SIZE: usize = std::mem::size_of::<usize>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

fn main() {
    fn print_prompt() {
        print!("db > ");
    }

    fn read_input() -> String {
        let mut input_buffer = String::new();
        io::stdin()
            .read_line(&mut input_buffer)
            .expect("Failed to read line");
        String::from(input_buffer.trim())
    }

    fn do_meta_command(command: &str, table: &mut Table) -> MetaCommandResult {
        if command.eq(".exit") {
            db_close(table);
            process::exit(0x0100);
        } else if command.eq(".constants") {
            println!("Constants:");
            print_constants();
            return MetaCommandResult::META_COMMAND_SUCCESS;
        } else if command.eq(".btree") {
            println!("Btree:");
            table.print_tree();
            return MetaCommandResult::META_COMMAND_SUCCESS;
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
        if pager.num_pages == 0 {
            unsafe {
                let root_node = pager.get_page(0);
                root_node.initialize_leaf_node();
                root_node.set_node_root(true);
            }
        }
        pager
    }

    fn db_open(file_name: &str) -> Table {
        let pager = pager_open(file_name);
        Table::new(pager)
    }

    fn db_close(table: &mut Table) {
        for i in 0..table.pager.num_pages {
            table.pager.pager_flush(i);
        }
    }

    fn prepare_insert(command: &str) -> Result<Box<Option<Statement>>, PrepareResult> {
        let splits: Vec<&str> = command.split(" ").collect();
        if splits.len() < 4 {
            return Err(PREPARE_SYNTAX_ERROR);
        }
        let id: i32 = splits[1].trim().parse().unwrap();
        if id < 0 {
            return Err(PREPARE_NEGATIVE_ID);
        }
        let id = id as u32;
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
                let (page_num, cell_num) = table.find(row_to_insert.id);
                let page = table.pager.get_page(page_num);
                if cell_num < page.leaf_node_num_cells() {
                    let key_at_index = page.leaf_node_key(cell_num);
                    if key_at_index == row_to_insert.id {
                        // TODO
                        return EXECUTE_DUPLICATE_KEY
                    }
                }
                let mut cursor = Cursor {
                    table,
                    page_num,
                    cell_num,
                    end_of_table: false
                };
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

    fn print_constants() {
        println!("ROW_SIZE: {}", ROW_SIZE);
        println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
        println!();
        println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
        println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
        println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
        println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
        println!();
        println!("INTERNAL_NODE_HEADER_SIZE: {}", INTERNAL_NODE_HEADER_SIZE);
        println!("INTERNAL_NODE_KEY_SIZE: {}", INTERNAL_NODE_KEY_SIZE);
        println!("INTERNAL_NODE_CHILD_SIZE: {}", INTERNAL_NODE_CHILD_SIZE);
        println!("INTERNAL_NODE_CELL_SIZE: {}", INTERNAL_NODE_CELL_SIZE);
    }

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Must supply a database filename.");
        process::exit(0x0100);
    }
    let mut table = db_open(args[1].as_str());
    loop {
        print_prompt();
        let command= read_input();
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
