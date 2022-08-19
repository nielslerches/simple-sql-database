use std::fs::File;
use std::io;
use std::ops::Index;

use csv;

use sqlparser::ast::{
    BinaryOperator, Expr, Query, SelectItem, SetExpr, Statement, TableFactor, Value as Literal,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Clone)]
enum Value {
    String(String),
    Boolean(bool),
    Integer(i64),
}

trait Relation: Iterator<Item = Vec<Value>> {
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
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.records().next() {
            Some(result) => match result {
                Ok(record) => {
                    let item = Vec::from_iter(record.iter().map(|s| s.to_owned()).map(|s| {
                        if let Ok(boolean) = s.parse::<bool>() {
                            Value::Boolean(boolean)
                        } else if let Ok(integer) = s.parse::<i64>() {
                            Value::Integer(integer)
                        } else {
                            Value::String(s)
                        }
                    }));
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
    projected: Vec<SelectItem>,
    relation: Box<dyn Relation<Item = Vec<Value>>>,
}

impl Iterator for Projection {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let relation_attributes: Vec<String> = self.relation.attributes();

        match self.relation.next() {
            Some(relation_item) => {
                let mut item = Vec::new();

                for select_item in self.projected.iter() {
                    if *select_item == SelectItem::Wildcard {
                        for attribute in &relation_attributes {
                            let source_position = relation_attributes
                                .iter()
                                .position(|relation_attribute| relation_attribute.eq(attribute))
                                .unwrap();

                            item.push(relation_item.index(source_position).clone());
                        }
                    } else {
                        let select_item_name = match select_item {
                            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                            SelectItem::UnnamedExpr(expr) => match expr {
                                Expr::Identifier(ident) => ident.value.clone(),
                                _ => unreachable!(),
                            },
                            _ => unimplemented!(),
                        };

                        let source_position = relation_attributes
                            .iter()
                            .position(|relation_attribute| relation_attribute.eq(&select_item_name))
                            .unwrap();

                        item.push(relation_item.index(source_position).clone());
                    }
                }

                Some(item)
            }
            None => None,
        }
    }
}

impl Relation for Projection {
    fn attributes(&mut self) -> Vec<String> {
        let mut attributes: Vec<String> = Vec::new();

        for select_item in self.projected.iter() {
            match select_item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    attributes.push(alias.value.clone());
                }
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => {
                        attributes.push(ident.value.clone());
                    }
                    _ => unimplemented!(),
                },
                SelectItem::Wildcard => {
                    for attribute in self.relation.attributes() {
                        attributes.push(attribute);
                    }
                }
                _ => unimplemented!(),
            }
        }

        attributes
    }
}

struct Selection {
    selection: Expr,
    relation: Box<dyn Relation<Item = Vec<Value>>>,
}

impl Iterator for Selection {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item: Option<Self::Item> = None;

        loop {
            match self.relation.next() {
                None => break,
                Some(relation_item) => {
                    // TODO: "compile" selection expr into callable and cache it.

                    if !eval_value_as_bool(eval_expr_on_row(self.selection.clone(), &self.relation.attributes(), &relation_item))
                    {
                        continue;
                    }

                    item = Some(relation_item);
                    break;
                }
            }
        }

        item
    }
}

impl Relation for Selection {
    fn attributes(&mut self) -> Vec<String> {
        self.relation.attributes()
    }
}

fn eval_expr_on_row(expr: Expr, relation_attributes: &Vec<String>, row: &Vec<Value>) -> Value {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_value = eval_expr_on_row(*left, relation_attributes, row);
            let right_value = eval_expr_on_row(*right, relation_attributes, row);

            match op {
                BinaryOperator::And => Value::Boolean(
                    eval_value_as_bool(left_value.into()) && eval_value_as_bool(right_value.into()),
                ),
                BinaryOperator::Or => Value::Boolean(
                    eval_value_as_bool(left_value.into()) || eval_value_as_bool(right_value.into()),
                ),
                BinaryOperator::Gt => Value::Boolean(match left_value {
                    Value::Integer(left_int) => {
                        left_int
                            > match right_value {
                                Value::Integer(right_int) => right_int,
                                _ => unimplemented!(),
                            }
                    }
                    _ => unimplemented!(),
                }),
                _ => unimplemented!(),
            }
        }
        Expr::Identifier(ident) => {
            let source_position = relation_attributes
                .iter()
                .position(|relation_attribute| relation_attribute.eq(&ident.value))
                .unwrap();

            (*row.index(source_position)).to_owned()
        }
        Expr::Value(literal) => match literal {
            Literal::Boolean(b) => Value::Boolean(b),
            Literal::DoubleQuotedString(s) | Literal::SingleQuotedString(s) => Value::String(s),
            Literal::Number(s, _) => {
                Value::Integer(s.parse::<i64>().expect("Could not parse number into i64."))
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!("{expr:?}"),
    }
}

fn eval_value_as_bool(value: Value) -> bool {
    match value {
        Value::Boolean(b) => b,
        Value::Integer(i) => i != 0,
        Value::String(s) => s.len() > 0,
    }
}

fn query_as_relation(query: &Box<Query>) -> Box<dyn Relation<Item = Vec<Value>> + 'static> {
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

                    let mut relation: Box<dyn Relation<Item = Vec<Value>> + 'static> =
                        Box::new(SequentialScan::from_path(&filename));

                    if let Some(selection) = &select.selection {
                        relation = Box::new(Selection {
                            selection: selection.to_owned(),
                            relation,
                        });
                    }

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

fn project_relation(
    projection: Vec<SelectItem>,
    relation: Box<dyn Relation<Item = Vec<Value>>>,
) -> Projection {
    Projection {
        projected: projection,
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
                let mut relation = query_as_relation(query);
                let attributes = relation.attributes();

                let mut writer = csv::Writer::from_writer(io::stdout());

                writer
                    .write_record(attributes)
                    .expect("Could not write CSV-header to STDOUT.");

                for row in relation {
                    let record = csv::StringRecord::from_iter(row.iter().map(|v| match v {
                        Value::String(s) => s.to_owned(),
                        Value::Boolean(b) => {
                            if *b {
                                "true".to_owned()
                            } else {
                                "false".to_owned()
                            }
                        }
                        Value::Integer(i) => i.to_string(),
                    }));
                    writer
                        .write_record(&record)
                        .expect("Could not write result to stdout.");
                }
            }
            _ => unimplemented!(),
        }
    }
}
