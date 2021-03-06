use std::fs::{File, OpenOptions};
use std::{env, io};
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::Rev;
use std::ops::Range;
use std::process;
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
        self.set_leaf_node_next_leaf(0);
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
        *(self.get_node_type()) == NodeType::NODE_LEAF
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
            *(self.index(INTERNAL_NODE_NUM_KEYS_OFFSET) as *const usize)
        }
    }

    pub fn increase_internal_node_num_keys(&mut self, incr: usize) {
        let origin_num_keys = self.get_internal_node_num_keys();
        self.set_internal_node_num_keys(origin_num_keys + incr);
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

    pub fn get_leaf_node_next_leaf(&self) -> usize {
        unsafe {
            *(self.index(LEAF_NODE_NEXT_LEAF_OFFSET) as *const usize)
        }
    }

    pub fn set_leaf_node_next_leaf(&self, next_leaf: usize) {
        unsafe {
            *(self.index(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut usize) = next_leaf;
        }
    }

    pub fn get_node_parent(&self) -> usize {
        unsafe {
            *(self.index(PARENT_POINTER_OFFSET) as *const usize)
        }
    }

    pub fn set_node_parent(&self, parent_page_num: usize) {
        unsafe {
            *(self.index(PARENT_POINTER_OFFSET) as *mut usize) = parent_page_num;
        }
    }

    pub fn update_internal_node_key(&mut self, old_key: u32, new_key: u32) {
        let old_child_index = self.internal_node_find_child(old_key);
        self.set_internal_node_key(old_child_index, new_key);
    }

    /// Return the index of the child which should contain the given key.
    fn internal_node_find_child(&self, key: u32) -> usize {
        let num_keys = self.get_internal_node_num_keys();
        // binary search
        let (mut min_cell, mut max_cell) = (0, num_keys);
        while min_cell < max_cell {
            let cell_num = (max_cell - min_cell) / 2 + min_cell;
            let cell_key_value = self.get_internal_node_key(cell_num);
            if cell_key_value >= key {
                max_cell = cell_num;
            } else {
                min_cell = cell_num + 1;
            }
        }
        max_cell
    }

    fn leaf_node_find(&self, key: u32) -> usize {
        let num_cells = self.leaf_node_num_cells();
        let (mut min_index, mut one_past_max_index) = (0, num_cells);
        while one_past_max_index != min_index {
            let index = (one_past_max_index + min_index) / 2;
            let key_at_index = self.leaf_node_key(index);
            if key_at_index == key {
                // return
                return index;
            } else if key_at_index > key {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }
}

pub struct Pager {
    file_descriptor: RefCell<File>,
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
            file_descriptor: RefCell::new(file),
            pages: std::iter::repeat_with(|| None).take(TABLE_MAX_PAGES).collect::<Vec<_>>()
        }
    }

    fn get_page_view(&self, page_num: usize) -> Option<&Page> {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }

        unsafe {
            let ptr = self.pages.as_ptr();
            let page = ptr.offset(page_num as isize);
            if (*page).is_none() {
                self.load_page(page_num);
            }
            let page = ptr.offset(page_num as isize);
            Some((*page).as_ref().unwrap().as_ref())
        }
    }

    fn load_page(&self, page_num: usize) {
        // create a page in memory
        let mut new_page = Page::new();
        if page_num <= self.num_pages {
            self.file_descriptor.borrow_mut().seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
            let result = self.file_descriptor.borrow_mut().read(&mut new_page.buf);
            if result.is_err() {
                println!("Error reading file: {}", result.unwrap());
                process::exit(0x0100);
            }
        }

        unsafe {
            let ptr = self.pages.as_ptr();
            let pages = ptr as *mut Option<Box<Page>>;
            (*pages.offset(page_num as isize)) = Some(Box::new(new_page));
        }
    }

    fn get_page(&mut self, page_num: usize) -> &mut Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }
        unsafe {
            let ptr = self.pages.as_ptr();
            let page = ptr.offset(page_num as isize);
            if (*page).is_none() {
                self.load_page(page_num);
                if page_num >= self.num_pages {
                    self.num_pages += 1;
                }
            }
        }
        let pages = self.pages.as_mut_ptr();
        unsafe {
            let page = pages.offset(page_num as isize);
            (*page).as_mut().unwrap().as_mut()
        }
    }

    /// Find the leftmost leaf page number.
    /// This implementation is different from the origin of the tutorial in which the implementation
    /// of finding the leftmost leaf page by finding the page of the lowest key residing. For example,
    /// by finding the key 0, and then return the page key 0 should be inserted.
    pub fn get_leftmost_leaf_page_num(&self, page_num: usize) -> usize {
        let page = self.get_page_view(page_num);
        if page.is_none() {
            panic!("invalid page number {}", page_num);
        }
        let p = page.unwrap();
        if p.is_leaf_node() {
            return page_num;
        }
        let child_page_num = p.get_internal_node_child(0);
        return self.get_leftmost_leaf_page_num(child_page_num);
    }

    pub fn pager_flush(&mut self, page_num: usize) {
        match &self.pages[page_num] {
            Some(page) => {
                self.file_descriptor.borrow_mut().seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64));
                self.file_descriptor.borrow_mut().write(page.buf.as_slice());
                self.file_descriptor.borrow_mut().flush();
            },
            None => ()

        }
    }

    fn close(&mut self) {
        self.file_descriptor.borrow_mut().flush();
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

    /// Find the position of the key in the table from root page to leaf page according.
    /// The position contains page number and cell number, if the key does not exist in any leaf
    /// page, then the position the key could be inserted will be returned.
    pub fn find(&self, key: u32) -> (usize, usize) {
        let root_page_num = self.root_page_num;
        let page = self.pager.get_page_view(root_page_num);
        if page.is_none() {
            return (0, 0);
        }
        self.find_by_page(page.unwrap(), key, root_page_num)
    }

    fn find_by_page_num(&self, page_num: usize, key: u32) -> (usize, usize) {
        let page = self.pager.get_page_view(page_num);
        if page.is_none() {
            println!("page {} not exist", page_num);
            process::exit(0x0010);
        }
        self.find_by_page(page.unwrap(), key, page_num)
    }

    /// Find the page number and cell number of the given key, the whole search process starts from
    /// the root page, which can be an internal node or a leaf node.
    fn find_by_page(&self, page: &Page, key: u32, page_num: usize) -> (usize, usize) {
        if *page.get_node_type() == NODE_LEAF {
            self.leaf_node_find(page, key, page_num)
        } else {
            self.internal_node_find(page, key)
        }
    }

    pub fn internal_node_find(&self, page: &Page, key: u32) -> (usize, usize) {
        let cell_index = page.internal_node_find_child(key);
        if page.get_internal_node_key(cell_index) >= key {
            let child_page_num = page.get_internal_node_child(cell_index);
            return self.find_by_page_num(child_page_num, key);
        }
        let right_child_num = page.get_internal_node_right_child();
        self.find_by_page_num(right_child_num, key)
    }

    /// Binary searches this leaf node(page) for the given key.
    ///
    /// If the value is found, then the page number and cell number are returned, if the given key
    /// is not found, then the position the key could be inserted is returned.
    fn leaf_node_find(&self, page: &Page, key: u32, page_num: usize) -> (usize, usize) {
        (page_num, page.leaf_node_find(key))
    }

    /// Add a new child/key pair to parent that corresponds to child
    pub fn internal_node_insert(&mut self, parent_page_num: usize, child_page_num: usize) {
        let child_max_key;
        {
            let child = self.pager.get_page_view(child_page_num).unwrap();
            child_max_key = child.get_node_max_key();
        }

        let right_child_page_num;
        let child_max_key_index;
        let origin_num_keys;
        {
            let parent = self.pager.get_page(parent_page_num);
            right_child_page_num = parent.get_internal_node_right_child();
            child_max_key_index = parent.internal_node_find_child(child_max_key);
            origin_num_keys = parent.get_internal_node_num_keys();
            if origin_num_keys >= INTERNAL_NODE_MAX_CELLS {
                println!("Need to implement splitting internal node");
                process::exit(0x0010);
            }
            parent.increase_internal_node_num_keys(1);
        }

        let right_child_max_key;
        {
            let right_child = self.pager.get_page_view(right_child_page_num).unwrap();
            right_child_max_key = right_child.get_node_max_key();
        }

        if child_max_key > right_child_max_key {
            let parent = self.pager.get_page(parent_page_num);
            // let parent_ptr = parent as *mut Page;
            parent.set_internal_node_right_child(child_page_num);
            parent.set_internal_node_child(origin_num_keys, right_child_page_num);
            parent.set_internal_node_key(origin_num_keys, right_child_max_key);
        } else {
            let parent = self.pager.get_page(parent_page_num);
            for i in (child_max_key_index + 1..=origin_num_keys).rev() {
                unsafe {
                    std::ptr::copy_nonoverlapping(parent.leaf_node_cell(i - 1),
                                                  parent.leaf_node_cell(i) as *mut u8,
                                                  INTERNAL_NODE_CELL_SIZE);
                }
            }
            parent.set_internal_node_child(child_max_key_index, child_page_num);
            parent.set_internal_node_key(child_max_key_index, child_max_key);
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

        let leaf_page_num = table.pager.get_leftmost_leaf_page_num(root_page_num);
        let leaf_node = table.pager.get_page_view(leaf_page_num).unwrap();
        let num_cells = leaf_node.leaf_node_num_cells();

        Cursor {
            table,
            cell_num: 0,
            page_num: leaf_page_num,
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
            /* Advance to next leaf node */
            let next_page_num = page.get_leaf_node_next_leaf();
            if next_page_num == 0 {
                /* This was rightmost leaf */
                self.end_of_table = true;
            } else {
                self.page_num = next_page_num;
                self.cell_num = 0;
            }
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
    /// Because of the reference borrow checker mechanism of Rust???only one mutable reference can be
    /// borrowed at one time, so the copy page data process should be splitted into two code block.
    fn leaf_node_split_and_insert(&mut self, key: u32, value: &Row) {
        // create a new right node
        let value_cell_num = self.cell_num;
        // page that will be created
        let new_page_num = self.table.pager.get_unused_page_num();
        let old_max;
        {
            let old_node = self.get_page_view().unwrap();
            old_max = old_node.get_node_max_key();
            let old_next_page_num = old_node.get_leaf_node_next_leaf();
            let old_node_parent_num = old_node.get_node_parent();
            let old_node_ptr = old_node as *const Page;
            // create a new node
            let new_node = self.table.pager.get_page(new_page_num);
            // init and copy cells to new right node from old node
            new_node.initialize_leaf_node();
            new_node.set_node_parent(old_node_parent_num);
            new_node.set_leaf_node_next_leaf(old_next_page_num);
            copy_page_data((LEAF_NODE_LEFT_SPLIT_COUNT..LEAF_NODE_MAX_CELLS + 1).rev(), old_node_ptr, new_node, key, value, value_cell_num);
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
            copy_page_data((0..LEAF_NODE_LEFT_SPLIT_COUNT).rev(), old_node as *const Page, old_node, key, value, value_cell_num);
            old_node.set_leaf_node_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);
            old_node.set_leaf_node_next_leaf(new_page_num);
        }

        if is_node_root {
            // The old leaf node is the root node, then a new root node should be created.
            self.create_new_node(new_page_num);
        } else {
            let old_node = self.get_page();
            let parent_page_num = old_node.get_node_parent();
            let new_max = old_node.get_node_max_key();
            let parent = self.table.pager.get_page(parent_page_num);
            parent.update_internal_node_key(old_max, new_max);
            self.table.internal_node_insert(parent_page_num, new_page_num);
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

        let root_page_num = self.table.root_page_num;
        {
            let left_child = self.table.pager.get_page(left_child_page_num);
            left_child.set_node_parent(root_page_num);
        }
        {
            let right_child = self.table.pager.get_page(right_child_page_num);
            right_child.set_node_parent(root_page_num);
        }
    }
}

unsafe fn serialize_row(cell: *mut u8, source: &Row) {
    std::ptr::write(cell as *mut u32, source.id);

    std::ptr::write((cell as usize + USERNAME_OFFSET) as *mut [u8; USERNAME_SIZE], [0 as u8; USERNAME_SIZE]);
    std::ptr::copy(source.username.as_ptr(), (cell as usize + USERNAME_OFFSET) as *mut u8, source.username.len());

    std::ptr::write((cell as usize + EMAIL_OFFSET) as *mut [u8; EMAIL_SIZE], [0 as u8; EMAIL_SIZE]);
    std::ptr::copy(source.email.as_ptr(), (cell as usize + EMAIL_OFFSET) as *mut u8, source.email.len());
}

fn copy_page_data(rang: Rev<Range<usize>>, src_ptr: *const Page, dst_page: &mut Page, key: u32, value: &Row, value_cell_num: usize) {
    for i in rang {
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        let destination = dst_page.leaf_node_cell(index_within_node);
        unsafe {
            if i == value_cell_num {
                dst_page.set_leaf_node_key(index_within_node, key);
                let destination = dst_page.leaf_node_value(index_within_node);
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
const LEAF_NODE_NEXT_LEAF_SIZE: usize = std::mem::size_of::<usize>();
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

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

// TODO for test, to be replaced with actual internal node cell number
const INTERNAL_NODE_MAX_CELLS: usize = 3;

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
            Ok(stmt) => {
                match execute_statement(stmt, &mut table) {
                    EXECUTE_SUCCESS => println!("Executed."),
                    EXECUTE_DUPLICATE_KEY => println!("Error: Duplicate key."),
                    EXECUTE_TABLE_FULL => println!("Error: Table full."),
                    _ => println!("Error: execute failed")
                }
            },
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
    }
}
