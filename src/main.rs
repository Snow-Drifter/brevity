mod error;
mod server;
mod store;
mod vfs;

use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use uuid::Uuid;

use error::Error;
use store::Store;

#[derive(Parser)]
#[command(name = "brevity", about = "Tag-based virtual filesystem")]
struct Cli {
    #[arg(long, default_value = ".brevity", global = true)]
    store: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Add {
        path: PathBuf,
        #[arg(long, value_delimiter = ',', required = true)]
        tags: Vec<String>,
    },

    Tag {
        id: Uuid,
        #[arg(long, value_delimiter = ',')]
        add: Vec<String>,
        #[arg(long, value_delimiter = ',')]
        rm: Vec<String>,
    },

    Ls {
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,
    },

    Serve {
        #[arg(long, default_value = "Z:")]
        mount: String,
    },
}

fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let store = Arc::new(Store::open(&cli.store).map_err(Error::from)?);

    match cli.command {
        Command::Add { path, tags } => {
            if !path.exists() {
                return Err(Error::PathNotFound(path));
            }
            let entry = store.import(&path, &tags)?;
            println!("Added {}", entry.name);
            println!("  id:   {}", entry.id);
            println!("  tags: {}", entry.tags.join(", "));
        }

        Command::Tag { id, add, rm } => {
            if add.is_empty() && rm.is_empty() {
                return Err(Error::TagArgsEmpty);
            }
            if !add.is_empty() {
                store.add_tags(id, &add)?;
            }
            if !rm.is_empty() {
                store.remove_tags(id, &rm)?;
            }
            let entry = store.load_entry(id)?;
            println!("{} → [{}]", entry.name, entry.tags.join(", "));
        }

        Command::Ls { tags } => {
            let files = store.query(&tags)?;
            if files.is_empty() {
                println!("(no files)");
            } else {
                for f in files {
                    println!("{}\t{}\t[{}]", f.id, f.name, f.tags.join(", "));
                }
            }
        }

        Command::Serve { mount } => {
            server::run(store, &mount)?;
        }
    }

    Ok(())
}
