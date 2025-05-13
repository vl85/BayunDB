use super::{Table, Column};
use super::schema::DataType;

pub fn add_column(table: &mut Table, column: Column) -> Result<(), String> {
    let col_name = column.name().to_string();
    if table.has_column(&col_name) {
        return Err(format!("Column {} already exists in table {}", col_name, table.name()));
    }
    let idx = table.columns().len();
    // SAFETY: Table struct must expose columns as mutable for this to work, or use internal mutability.
    // Here, we assume columns is public or use a method to push.
    table.columns.push(column);
    table.column_map.insert(col_name, idx);
    if table.columns[idx].is_primary_key() {
        table.primary_key_columns.push(idx);
    }
    // TODO: Data migration - update all existing rows to have the default value for this column if present
    Ok(())
}

pub fn drop_column(table: &mut Table, column_name: &str) -> Result<(), String> {
    if !table.has_column(column_name) {
        return Err(format!("Column {} does not exist in table {}", column_name, table.name()));
    }
    let idx = table.column_map[column_name];
    table.columns.remove(idx);
    table.column_map.remove(column_name);
    // Rebuild column_map and primary_key_columns
    table.column_map.clear();
    table.primary_key_columns.clear();
    for (i, col) in table.columns.iter().enumerate() {
        table.column_map.insert(col.name().to_string(), i);
        if col.is_primary_key() {
            table.primary_key_columns.push(i);
        }
    }
    Ok(())
}

pub fn rename_column(table: &mut Table, old_name: &str, new_name: &str) -> Result<(), String> {
    if !table.has_column(old_name) {
        return Err(format!("Column {} does not exist in table {}", old_name, table.name()));
    }
    if table.has_column(new_name) {
        return Err(format!("Column {} already exists in table {}", new_name, table.name()));
    }
    let idx = table.column_map[old_name];
    table.columns[idx].rename(new_name);
    table.column_map.remove(old_name);
    table.column_map.insert(new_name.to_string(), idx);
    Ok(())
}

pub fn alter_column_type(table: &mut Table, column_name: &str, new_type: DataType) -> Result<(), String> {
    if let Some(&idx) = table.column_map.get(column_name) {
        if let Some(col) = table.columns.get_mut(idx) {
            col.set_data_type(new_type);
            Ok(())
        } else {
            Err(format!("Internal catalog error: Column index {} for '{}' out of bounds.", idx, column_name))
        }
    } else {
        Err(format!("Column '{}' not found in table '{}' for ALTER COLUMN TYPE.", column_name, table.name()))
    }
} 