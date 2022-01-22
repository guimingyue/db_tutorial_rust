use std::io;
use std::process;

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

    loop {
        print_prompt();
        let command = read_input();
        if command.eq(".exit") {
            process::exit(0x0100);
        } else {
            println!("Unrecognized command {}", command);
        }
    }
}
