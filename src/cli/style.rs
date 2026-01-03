use colored::*;

pub const LOGO: &str = r#"
 ╦  ┌─┐┌─┐╦ ╦┌─┐┬  ┬┌─┐┌┬┐
 ║  │ ││ ┬╠═╣├─┤└┐┌┘├┤ │││
 ╩═╝└─┘└─┘╩ ╩┴ ┴ └┘ └─┘┴ ┴
"#;

pub const TAGLINE: &str = "Local-first observability runtime";

pub fn print_banner() {
    println!("{}", LOGO.cyan().bold());
    println!("{}\n", TAGLINE.bright_black());
}

pub fn success(msg: &str) {
    println!("{} {}", "✓".green().bold(), msg);
}

pub fn error(msg: &str) {
    eprintln!("{} {}", "✗".red().bold(), msg);
}

pub fn info(msg: &str) {
    println!("{} {}", "ℹ".blue().bold(), msg);
}

pub fn warning(msg: &str) {
    println!("{} {}", "⚠".yellow().bold(), msg);
}

pub fn step(msg: &str) {
    println!("{} {}", "→".cyan(), msg);
}
