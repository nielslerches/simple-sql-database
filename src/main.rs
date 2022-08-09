use std::fs::File;
use std::io;
use std::ops::Index;

use csv;

use sqlparser::ast::{Query, SetExpr, Statement, TableFactor, SelectItem, Expr};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

trait Relation: Iterator<Item = Vec<String>> {
    fn attributes(&mut self) -> Vec<String>;
}

struct SequentialScan {
    reader: csv::Reader<File>,
}

impl SequentialScan {
    pub fn from_path(path: &String) -> Self {
        let reader = csv::Reader::from_path(path)
            .expect(format!("Could not create CSV-reader from path: {}", path).as_str());

        Self { reader }
    }
}

impl Iterator for SequentialScan {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.records().next() {
            Some(result) => match result {
                Ok(record) => {
                    let item = Vec::from_iter(record.iter().map(|s| s.to_owned()));
                    Some(item)
                }
                Err(err) => {
                    eprintln!("{err}");
                    None
                }
            },
            None => None,
        }
    }
}

impl Relation for SequentialScan {
    fn attributes(&mut self) -> Vec<String> {
        let headers = self
            .reader
            .headers()
            .expect("Could not get headers from CSV-reader.");

        Vec::from_iter(headers.iter().map(|s| s.to_owned()))
    }
}

struct Projection {
    projected: Vec<(String, String)>,
    relation: Box<dyn Relation<Item = Vec<String>>>,
}

impl Iterator for Projection {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let relation_attributes: Vec<String> = self.relation.attributes();

        match self.relation.next() {
            Some(relation_item) => {
                let mut item = Vec::new();

                for (source, _) in self.projected.iter() {
                    let source_position = relation_attributes
                        .iter()
                        .position(|relation_attribute| relation_attribute.eq(source))
                        .unwrap();

                    item.push(relation_item.index(source_position).clone());
                }

                Some(item)
            }
            None => None,
        }
    }
}

impl Relation for Projection {
    fn attributes(&mut self) -> Vec<String> {
        Vec::from_iter(self.projected.iter().map(|(_, target)| target.clone()))
    }
}

fn query_as_relation(query: &Box<Query>) -> Box<dyn Relation> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            let table_with_joins = select.from.first().expect("FROM must be provided.");

            if !table_with_joins.joins.is_empty() {
                unimplemented!("JOIN is not supported.")
            }

            let table_factor = &table_with_joins.relation;

            match table_factor {
                TableFactor::Table { name, .. } => {
                    let filename = name
                        .0
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<String>>()
                        .join(".");

                    let mut relation: Box<dyn Relation<Item = Vec<String>>> = Box::new(SequentialScan::from_path(&filename));

                    if !select.projection.is_empty() {
                        relation = Box::new(project_relation(select.projection.clone(), relation));
                    }

                    return relation;
                }
                _ => {
                    unimplemented!()
                }
            }
        }
        _ => {
            unimplemented!()
        }
    }
}

fn project_relation(projection: Vec<SelectItem>, relation: Box<dyn Relation<Item = Vec<String>>>) -> Projection {
    Projection {
        projected: projection.iter().map(|select_item| {
            match select_item {
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        Expr::Identifier(ident) => {
                            (ident.value.clone(), ident.value.clone())
                        },
                        _ => {
                            unimplemented!()
                        }
                    }
                },
                _ => {
                    unimplemented!()
                }
            }
        }).collect(),
        relation,
    }
}

fn main() {
    let dialect = GenericDialect {};

    let mut sql = String::new();

    for line in io::stdin().lines() {
        let line = line.expect("Could not read line from STDIN.");
        sql.push_str(line.as_str());
    }

    let ast = Parser::parse_sql(&dialect, sql.as_str()).expect("Could not parse SQL.");

    for statement in ast.iter() {
        match statement {
            Statement::Query(query) => {
                let relation = query_as_relation(query);

                let mut writer = csv::Writer::from_writer(io::stdout());

                for row in relation {
                    let record = csv::StringRecord::from(row);
                    writer
                        .write_record(&record)
                        .expect("Could not write result to stdout.");
                }
            },
            _ => {
                unimplemented!()
            }
        }
    }
}
