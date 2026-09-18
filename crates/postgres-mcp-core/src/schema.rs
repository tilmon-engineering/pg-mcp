//! Fixed, role-visible PostgreSQL catalog introspection.
//!
//! Schema inspection is deliberately separate from user transactions.  The
//! worker starts one repeatable-read, read-only transaction, runs these fixed
//! catalog statements in order, and commits only after the complete bounded
//! response has been assembled.  Catalog values (including defaults and
//! constraint definitions) are data, never executable guidance.

use crate::config::Config;
use crate::libpq::{self, PollControl, QueryLimits, ResultSet};
use crate::protocol::{
    Constraint, ConstraintKind, IdentityKind, Relation, RelationKind, SchemaColumn, SchemaInfo,
    SchemaResult,
};
use std::collections::BTreeMap;
use std::time::Instant;

/// One query per metadata family keeps the SQL fixed while allowing the result
/// builder to enforce one aggregate schema budget across all returned objects.
pub const RELATIONS_SQL: &str = r#"
SELECT n.nspname, c.relname, c.relkind::text, c.oid::text
FROM pg_namespace AS n
JOIN pg_class AS c ON c.relnamespace = n.oid
WHERE has_schema_privilege(n.oid, 'USAGE')
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_'
  AND c.relkind IN ('r','p','v','m','f')
  AND (
    has_table_privilege(c.oid, 'SELECT')
    OR has_table_privilege(c.oid, 'INSERT')
    OR has_table_privilege(c.oid, 'UPDATE')
    OR has_table_privilege(c.oid, 'DELETE')
    OR EXISTS (
      SELECT 1 FROM pg_attribute AS pa
      WHERE pa.attrelid = c.oid AND pa.attnum > 0 AND NOT pa.attisdropped
        AND (has_column_privilege(c.oid, pa.attnum, 'SELECT')
          OR has_column_privilege(c.oid, pa.attnum, 'INSERT')
          OR has_column_privilege(c.oid, pa.attnum, 'UPDATE'))
    )
  )
ORDER BY n.nspname, c.relname, c.oid
"#;

pub const COLUMNS_SQL: &str = r#"
SELECT n.nspname,
       c.relname,
       a.attname,
       a.attnum::text,
       a.atttypid::text,
       format_type(a.atttypid, a.atttypmod),
       CASE WHEN a.attnotnull THEN 'f' ELSE 't' END,
       pg_get_expr(ad.adbin, ad.adrelid),
       a.attidentity::text,
       CASE WHEN a.attgenerated <> '' THEN 't' ELSE 'f' END
FROM pg_namespace AS n
JOIN pg_class AS c ON c.relnamespace = n.oid
JOIN pg_attribute AS a ON a.attrelid = c.oid
LEFT JOIN pg_attrdef AS ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE has_schema_privilege(n.oid, 'USAGE')
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_'
  AND c.relkind IN ('r','p','v','m','f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND (
    has_table_privilege(c.oid, 'SELECT')
    OR has_table_privilege(c.oid, 'INSERT')
    OR has_table_privilege(c.oid, 'UPDATE')
    OR has_column_privilege(c.oid, a.attnum, 'SELECT')
    OR has_column_privilege(c.oid, a.attnum, 'INSERT')
    OR has_column_privilege(c.oid, a.attnum, 'UPDATE')
  )
ORDER BY n.nspname, c.relname, a.attnum
"#;

pub const CONSTRAINTS_SQL: &str = r#"
SELECT n.nspname,
       c.relname,
       con.conname,
       con.contype::text,
       pg_get_constraintdef(con.oid, true)
FROM pg_namespace AS n
JOIN pg_class AS c ON c.relnamespace = n.oid
JOIN pg_constraint AS con ON con.conrelid = c.oid
WHERE has_schema_privilege(n.oid, 'USAGE')
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_'
  AND c.relkind IN ('r','p','v','m','f')
  AND (
    has_table_privilege(c.oid, 'SELECT')
    OR has_table_privilege(c.oid, 'INSERT')
    OR has_table_privilege(c.oid, 'UPDATE')
    OR has_table_privilege(c.oid, 'DELETE')
  )
ORDER BY n.nspname, c.relname, con.conname
"#;

#[derive(Debug)]
pub(crate) enum SchemaError {
    Backend(libpq::AdapterError),
    Limit,
    Malformed,
}

/// Retrieve and validate the complete bounded schema snapshot.
pub(crate) fn retrieve(
    connection: &mut libpq::Connection,
    config: &Config,
    deadline: Instant,
    control: &dyn PollControl,
) -> Result<SchemaResult, SchemaError> {
    let unlimited = QueryLimits {
        sql_bytes: config.sql_bytes,
        parameters: config.parameters,
        parameter_bytes: config.parameter_bytes,
        result_rows: config.result_rows,
        columns: config.columns.max(10),
        cell_bytes: config.cell_bytes,
        result_json_bytes: usize::MAX,
    };
    execute_checked(
        connection,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        deadline,
        control,
    )?;
    let result = retrieve_inner(connection, config, unlimited, deadline, control);
    match result {
        Ok(snapshot) => {
            if let Err(error) = execute_checked(connection, "COMMIT", deadline, control) {
                let _ = connection.execute("ROLLBACK", deadline, control);
                return Err(error);
            }
            Ok(snapshot)
        }
        Err(error) => {
            let _ = connection.execute("ROLLBACK", deadline, control);
            Err(error)
        }
    }
}

fn retrieve_inner(
    connection: &mut libpq::Connection,
    config: &Config,
    limits: QueryLimits,
    deadline: Instant,
    control: &dyn PollControl,
) -> Result<SchemaResult, SchemaError> {
    let relations = query(connection, RELATIONS_SQL, limits, deadline, control)?;
    let columns = query(connection, COLUMNS_SQL, limits, deadline, control)?;
    let constraints = query(connection, CONSTRAINTS_SQL, limits, deadline, control)?;

    let mut schemas: BTreeMap<String, BTreeMap<String, Relation>> = BTreeMap::new();
    for row in relations.rows {
        if row.len() != 4 {
            return Err(SchemaError::Malformed);
        }
        let schema = required(&row, 0)?;
        let relation = required(&row, 1)?;
        let kind = relation_kind(required(&row, 2)?)?;
        // The OID is selected as a stable, server-local disambiguator and is
        // intentionally not returned; validating its decimal form catches a
        // malformed adapter result without leaking backend diagnostics.
        let _oid = required(&row, 3)?
            .parse::<u32>()
            .map_err(|_| SchemaError::Malformed)?;
        schemas
            .entry(schema.to_owned())
            .or_default()
            .entry(relation.to_owned())
            .or_insert(Relation {
                name: relation.to_owned(),
                kind,
                columns: Vec::new(),
                constraints: Vec::new(),
            });
        validate_partial(&schemas, config)?;
    }

    for row in columns.rows {
        if row.len() != 10 {
            return Err(SchemaError::Malformed);
        }
        let schema = required(&row, 0)?;
        let relation = required(&row, 1)?;
        let Some(relation) = schemas
            .get_mut(schema)
            .and_then(|relations| relations.get_mut(relation))
        else {
            // A concurrent catalog change cannot occur inside this snapshot;
            // treat inconsistent server output as an internal bounded failure.
            return Err(SchemaError::Malformed);
        };
        let ordinal = required(&row, 3)?
            .parse::<u32>()
            .map_err(|_| SchemaError::Malformed)?;
        let type_oid = required(&row, 4)?
            .parse::<u32>()
            .map_err(|_| SchemaError::Malformed)?;
        let nullable = parse_bool(required(&row, 6)?)?;
        let identity = match optional(&row, 8) {
            None | Some("") => IdentityKind::None,
            Some("a") => IdentityKind::Always,
            Some("d") => IdentityKind::ByDefault,
            Some(_) => return Err(SchemaError::Malformed),
        };
        relation.columns.push(SchemaColumn {
            name: required(&row, 2)?.to_owned(),
            ordinal,
            type_oid,
            type_name: required(&row, 5)?.to_owned(),
            nullable,
            default_expression: optional(&row, 7).map(str::to_owned),
            identity,
            generated: parse_bool(required(&row, 9)?)?,
        });
        validate_partial(&schemas, config)?;
    }

    for row in constraints.rows {
        if row.len() != 5 {
            return Err(SchemaError::Malformed);
        }
        let schema = required(&row, 0)?;
        let relation = required(&row, 1)?;
        let Some(relation) = schemas
            .get_mut(schema)
            .and_then(|relations| relations.get_mut(relation))
        else {
            return Err(SchemaError::Malformed);
        };
        relation.constraints.push(Constraint {
            name: required(&row, 2)?.to_owned(),
            kind: constraint_kind(required(&row, 3)?)?,
            definition: required(&row, 4)?.to_owned(),
        });
        validate_partial(&schemas, config)?;
    }

    let output = SchemaResult {
        schemas: schemas
            .into_iter()
            .filter_map(|(name, relations)| {
                if relations.is_empty() {
                    None
                } else {
                    let mut relations: Vec<_> = relations.into_values().collect();
                    for relation in &mut relations {
                        relation.columns.sort_by_key(|column| column.ordinal);
                        relation
                            .constraints
                            .sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
                    }
                    Some(SchemaInfo { name, relations })
                }
            })
            .collect(),
    };
    // SchemaResult::validate counts schema/relation/column/constraint units and
    // checks canonical aggregate JSON bytes. It is called only after all rows
    // have been assembled and before the repeatable-read transaction commits.
    output.validate(config).map_err(|error| match error {
        crate::protocol::ValidationError::SchemaLimit
        | crate::protocol::ValidationError::ResultLimit => SchemaError::Limit,
        _ => SchemaError::Malformed,
    })?;
    Ok(output)
}

/// Build a sorted public snapshot from the currently retained aggregate maps.
/// This clone is deliberate: it lets limit validation reject the next object
/// before a larger catalog response is retained permanently.
fn schema_from_maps(schemas: &BTreeMap<String, BTreeMap<String, Relation>>) -> SchemaResult {
    SchemaResult {
        schemas: schemas
            .iter()
            .filter_map(|(name, relations)| {
                if relations.is_empty() {
                    None
                } else {
                    let mut relations: Vec<_> = relations.values().cloned().collect();
                    for relation in &mut relations {
                        relation.columns.sort_by_key(|column| column.ordinal);
                        relation
                            .constraints
                            .sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
                    }
                    Some(SchemaInfo {
                        name: name.clone(),
                        relations,
                    })
                }
            })
            .collect(),
    }
}

fn validate_partial(
    schemas: &BTreeMap<String, BTreeMap<String, Relation>>,
    config: &Config,
) -> Result<(), SchemaError> {
    schema_from_maps(schemas)
        .validate(config)
        .map_err(|error| match error {
            crate::protocol::ValidationError::SchemaLimit
            | crate::protocol::ValidationError::ResultLimit => SchemaError::Limit,
            _ => SchemaError::Malformed,
        })
}

fn query(
    connection: &mut libpq::Connection,
    sql: &str,
    limits: QueryLimits,
    deadline: Instant,
    control: &dyn PollControl,
) -> Result<ResultSet, SchemaError> {
    connection
        .query(sql, &[], limits, deadline, control)
        .map_err(SchemaError::Backend)
}

fn execute_checked(
    connection: &mut libpq::Connection,
    sql: &str,
    deadline: Instant,
    control: &dyn PollControl,
) -> Result<(), SchemaError> {
    let result = connection
        .execute(sql, deadline, control)
        .map_err(SchemaError::Backend)?;
    if result.command_tag == sql.split_whitespace().next().unwrap_or_default() {
        Ok(())
    } else {
        Err(SchemaError::Malformed)
    }
}

fn required(row: &[Option<String>], index: usize) -> Result<&str, SchemaError> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or(SchemaError::Malformed)
}

fn optional(row: &[Option<String>], index: usize) -> Option<&str> {
    row.get(index).and_then(Option::as_deref)
}

fn parse_bool(value: &str) -> Result<bool, SchemaError> {
    match value {
        "t" => Ok(true),
        "f" => Ok(false),
        _ => Err(SchemaError::Malformed),
    }
}

fn relation_kind(value: &str) -> Result<RelationKind, SchemaError> {
    match value {
        "r" => Ok(RelationKind::Table),
        "p" => Ok(RelationKind::PartitionedTable),
        "v" => Ok(RelationKind::View),
        "m" => Ok(RelationKind::MaterializedView),
        "f" => Ok(RelationKind::ForeignTable),
        _ => Err(SchemaError::Malformed),
    }
}

fn constraint_kind(value: &str) -> Result<ConstraintKind, SchemaError> {
    match value {
        "p" => Ok(ConstraintKind::PrimaryKey),
        "u" => Ok(ConstraintKind::Unique),
        "f" => Ok(ConstraintKind::ForeignKey),
        "c" => Ok(ConstraintKind::Check),
        "x" => Ok(ConstraintKind::Exclusion),
        _ => Err(SchemaError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_queries_are_fixed_and_role_scoped() {
        for sql in [RELATIONS_SQL, COLUMNS_SQL, CONSTRAINTS_SQL] {
            assert!(sql.contains("has_schema_privilege"));
            assert!(sql.contains("has_table_privilege"));
            assert!(sql.contains("pg_namespace"));
            assert!(!sql.contains('$'));
        }
        assert!(!RELATIONS_SQL.contains("REPEATABLE"));
    }

    #[test]
    fn relation_and_constraint_kinds_are_exhaustive() {
        assert!(matches!(relation_kind("r"), Ok(RelationKind::Table)));
        assert!(matches!(
            relation_kind("p"),
            Ok(RelationKind::PartitionedTable)
        ));
        assert!(matches!(relation_kind("v"), Ok(RelationKind::View)));
        assert!(matches!(
            relation_kind("m"),
            Ok(RelationKind::MaterializedView)
        ));
        assert!(matches!(relation_kind("f"), Ok(RelationKind::ForeignTable)));
        assert!(matches!(
            constraint_kind("p"),
            Ok(ConstraintKind::PrimaryKey)
        ));
        assert!(matches!(constraint_kind("u"), Ok(ConstraintKind::Unique)));
        assert!(matches!(
            constraint_kind("f"),
            Ok(ConstraintKind::ForeignKey)
        ));
        assert!(matches!(constraint_kind("c"), Ok(ConstraintKind::Check)));
        assert!(matches!(
            constraint_kind("x"),
            Ok(ConstraintKind::Exclusion)
        ));
    }

    #[test]
    fn booleans_and_identity_values_are_strict() {
        assert!(parse_bool("t").unwrap());
        assert!(!parse_bool("f").unwrap());
        assert!(parse_bool("true").is_err());
        assert!(matches!(
            match "a" {
                "a" => Ok(IdentityKind::Always),
                _ => Err(SchemaError::Malformed),
            },
            Ok(IdentityKind::Always)
        ));
    }
}
