// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Scaffolding.Internal
{
    /// <summary>
    ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
    ///     directly from your code. This API may change or be removed in future releases.
    /// </summary>
    public class SqlServerDatabaseModelFactory : IDatabaseModelFactory
    {
        private readonly IDiagnosticsLogger<DbLoggerCategory.Scaffolding> _logger;
        private static readonly ISet<string> _dateTimePrecisionTypes = new HashSet<string> { "datetimeoffset", "datetime2", "time" };

        private static readonly ISet<string> _maxLengthRequiredTypes
            = new HashSet<string> { "binary", "varbinary", "char", "varchar", "nchar", "nvarchar" };

        // see https://msdn.microsoft.com/en-us/library/ff878091.aspx
        // decimal/numeric are excluded because default value varies based on the precision.
        private static readonly Dictionary<string, long[]> _defaultSequenceMinMax = new Dictionary<string, long[]>(StringComparer.OrdinalIgnoreCase)
        {
            { "tinyint", new[] { 0L, 255L } },
            { "smallint", new[] { -32768L, 32767L } },
            { "int", new[] { -2147483648L, 2147483647L } },
            { "bigint", new[] { -9223372036854775808L, 9223372036854775807L } }
        };

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public SqlServerDatabaseModelFactory([NotNull] IDiagnosticsLogger<DbLoggerCategory.Scaffolding> logger)
        {
            Check.NotNull(logger, nameof(logger));

            _logger = logger;
        }

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public virtual DatabaseModel Create(string connectionString, IEnumerable<string> tables, IEnumerable<string> schemas)
        {
            Check.NotEmpty(connectionString, nameof(connectionString));
            Check.NotNull(tables, nameof(tables));
            Check.NotNull(schemas, nameof(schemas));

            using (var connection = new SqlConnection(connectionString))
            {
                return Create(connection, tables, schemas);
            }
        }

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public virtual DatabaseModel Create(DbConnection connection, IEnumerable<string> tables, IEnumerable<string> schemas)
        {
            Check.NotNull(connection, nameof(connection));
            Check.NotNull(tables, nameof(tables));
            Check.NotNull(schemas, nameof(schemas));

            var databaseModel = new DatabaseModel();

            var connectionStartedOpen = connection.State == ConnectionState.Open;
            if (!connectionStartedOpen)
            {
                connection.Open();
            }
            try
            {
                databaseModel.DatabaseName = connection.Database;
                databaseModel.DefaultSchema = GetDefaultSchema(connection);

                var typeAliases = GetTypeAliases(connection);
                var selectionSet = new SelectionSet(tables, schemas);

                if (Version.TryParse(connection.ServerVersion, out var serverVersion)
                    && serverVersion.Major >= 11)
                {
                    foreach (var sequence in GetSequences(connection, selectionSet, typeAliases))
                    {
                        sequence.Database = databaseModel;
                        databaseModel.Sequences.Add(sequence);
                    }
                }

                foreach (var table in GetTables(connection, selectionSet, typeAliases))
                {
                    table.Database = databaseModel;
                    databaseModel.Tables.Add(table);
                }

                foreach (var table in databaseModel.Tables)
                {
                    foreach (var foreignKey in GetForeignKeys(connection, table, databaseModel.Tables))
                    {
                        foreignKey.Table = table;
                        table.ForeignKeys.Add(foreignKey);
                    }
                }

                foreach (var schema in selectionSet.GetMissingSchemas())
                {
                    _logger.MissingSchemaWarning(schema);
                }

                foreach (var table in selectionSet.GetMissingTables())
                {
                    _logger.MissingTableWarning(table);
                }

                return databaseModel;
            }
            finally
            {
                if (!connectionStartedOpen)
                {
                    connection.Close();
                }
            }
        }

        private string GetDefaultSchema(DbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT schema_name()";

                if (command.ExecuteScalar() is string schema)
                {
                    _logger.DefaultSchemaFound(schema);

                    return schema;
                }

                return null;
            }
        }

        private class SelectionSet
        {
            private static readonly List<string> _schemaPatterns = new List<string>
            {
                "{schema}",
                "[{schema}]"
            };

            private static readonly List<string> _tablePatterns = new List<string>
            {
                "{schema}.{table}",
                "[{schema}].[{table}]",
                "{schema}.[{table}]",
                "[{schema}].{table}",
                "{table}",
                "[{table}]"
            };

            private readonly HashSet<string> _tablesToSelect;
            private readonly HashSet<string> _schemasToSelect;
            private readonly HashSet<string> _selectedTables;
            private readonly HashSet<string> _selectedSchemas;

            public SelectionSet(IEnumerable<string> tables, IEnumerable<string> schemas)
            {
                _tablesToSelect = new HashSet<string>(tables, StringComparer.OrdinalIgnoreCase);
                _schemasToSelect = new HashSet<string>(schemas, StringComparer.OrdinalIgnoreCase);
                _selectedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _selectedSchemas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            public bool Allows(string schema, string table)
            {
                if (_tablesToSelect.Count == 0
                    && _schemasToSelect.Count == 0)
                {
                    return true;
                }

                foreach (var schemaPattern in _schemaPatterns)
                {
                    var key = schemaPattern.Replace("{schema}", schema);
                    if (_schemasToSelect.Contains(key))
                    {
                        _selectedSchemas.Add(schema);
                        return true;
                    }
                }

                if (table != null)
                {
                    foreach (var tablePattern in _tablePatterns)
                    {
                        var key = tablePattern.Replace("{schema}", schema).Replace("{table}", table);
                        if (_tablesToSelect.Contains(key))
                        {
                            _selectedTables.Add(key);
                            return true;
                        }
                    }
                }

                return false;
            }

            public IEnumerable<string> GetMissingSchemas()
            {
                foreach (var schema in _schemasToSelect.Except(_selectedSchemas, StringComparer.OrdinalIgnoreCase))
                {
                    yield return schema;
                }
            }

            public IEnumerable<string> GetMissingTables()
            {
                foreach (var schema in _tablesToSelect.Except(_selectedTables, StringComparer.OrdinalIgnoreCase))
                {
                    yield return schema;
                }
            }
        }

        private IReadOnlyDictionary<string, string> GetTypeAliases(DbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                var typeAliasMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                command.CommandText = @"
SELECT
    schema_name([t].[schema_id]) AS [schema_name],
    [t].[name] AS [type_name],
    [t2].[name] AS [underlying_system_type],
    CAST([t].[max_length] AS int) AS [max_length],
    CAST([t].[precision] AS int) AS [precision],
    CAST([t].[scale] AS int) AS [scale]
FROM [sys].[types] AS [t]
JOIN [sys].[types] AS [t2] ON [t].[system_type_id] = [t2].[user_type_id]
WHERE [t].[is_user_defined] = 1";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schema = reader.GetValueOrDefault<string>("schema_name");
                        var userType = reader.GetValueOrDefault<string>("type_name");
                        var systemType = reader.GetValueOrDefault<string>("underlying_system_type");
                        var maxLength = reader.GetValueOrDefault<int>("max_length");
                        var precision = reader.GetValueOrDefault<int>("precision");
                        var scale = reader.GetValueOrDefault<int>("scale");

                        var storeType = GetStoreType(systemType, maxLength, precision, scale);

                        _logger.TypeAliasFound(DisplayName(schema, userType), storeType);

                        typeAliasMap.Add($"[{schema}].[{userType}]", storeType);
                    }
                }

                return typeAliasMap;
            }
        }

        private IEnumerable<DatabaseSequence> GetSequences(DbConnection connection, SelectionSet selectionSet, IReadOnlyDictionary<string, string> typeAliases)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
SELECT
    OBJECT_SCHEMA_NAME([s].[object_id]) AS [schema_name],
	[s].[name],
    schema_name([t].[schema_id]) AS [type_schema],
	type_name([s].[user_type_id]) AS [type_name],
	CAST([s].[precision] AS int) AS [precision],
    CAST([s].[scale] AS int) AS [scale],
    [s].[is_cycling],
	CAST([s].[increment] AS int) AS [increment],
	CAST([s].[start_value] AS bigint) AS [start_value],
    CAST([s].[minimum_value] AS bigint) AS [minimum_value],
    CAST([s].[maximum_value] AS bigint) AS [maximum_value]
FROM [sys].[sequences] AS [s]
JOIN [sys].[types] AS [t] ON [s].[user_type_id] = [t].[user_type_id]";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schema = reader.GetValueOrDefault<string>("schema_name");

                        if (!selectionSet.Allows(schema, table: null))
                        {
                            continue;
                        }

                        var name = reader.GetValueOrDefault<string>("name");
                        var storeTypeSchema = reader.GetValueOrDefault<string>("type_schema");
                        var storeType = reader.GetValueOrDefault<string>("type_name");
                        var precision = reader.GetValueOrDefault<int>("precision");
                        var scale = reader.GetValueOrDefault<int>("scale");
                        var isCyclic = reader.GetValueOrDefault<bool>("is_cycling");
                        var incrementBy = reader.GetValueOrDefault<int>("increment");
                        var startValue = reader.GetValueOrDefault<long>("start_value");
                        var minValue = reader.GetValueOrDefault<long>("minimum_value");
                        var maxValue = reader.GetValueOrDefault<long>("maximum_value");

                        // Swap store type if type alias is used
                        if (typeAliases.TryGetValue($"[{storeTypeSchema}].[{storeType}]", out var underlyingType))
                        {
                            storeType = underlyingType;
                        }

                        storeType = GetStoreType(storeType, maxLength: 0, precision: precision, scale: scale);

                        _logger.SequenceFound(DisplayName(schema, name), storeType, isCyclic, incrementBy, startValue, minValue, maxValue);

                        var sequence = new DatabaseSequence
                        {
                            Schema = schema,
                            Name = name,
                            StoreType = storeType,
                            IsCyclic = isCyclic,
                            IncrementBy = incrementBy,
                            StartValue = startValue,
                            MinValue = minValue,
                            MaxValue = maxValue
                        };

                        if (_defaultSequenceMinMax.ContainsKey(storeType))
                        {
                            var defaultMin = _defaultSequenceMinMax[storeType][0];
                            sequence.MinValue = sequence.MinValue == defaultMin ? null : sequence.MinValue;
                            sequence.StartValue = sequence.StartValue == defaultMin ? null : sequence.StartValue;

                            sequence.MaxValue = sequence.MaxValue == _defaultSequenceMinMax[sequence.StoreType][1] ? null : sequence.MaxValue;
                        }

                        yield return sequence;
                    }
                }
            }
        }

        private IEnumerable<DatabaseTable> GetTables(DbConnection connection, SelectionSet selectionSet, IReadOnlyDictionary<string, string> typeAliases)
        {
            using (var command = connection.CreateCommand())
            {
                var tables = new List<DatabaseTable>();
                Version.TryParse(connection.ServerVersion, out var serverVersion);
                var supportsMemoryOptimizedTable = serverVersion?.Major >= 12;
                var supportsTemporalTable = serverVersion?.Major >= 13;

                var commandText = @"
SELECT
    schema_name([t].[schema_id]) AS [schema],
    [t].[name]";

                if (supportsMemoryOptimizedTable)
                {
                    commandText += @",
    [t].[is_memory_optimized]";
                }

                commandText += @"
FROM [sys].[tables] AS [t]
WHERE [t].[is_ms_shipped] = 0
AND NOT EXISTS (SELECT *
    FROM [sys].[extended_properties] AS [ep]
    WHERE [ep].[major_id] = [t].[object_id]
        AND [ep].[minor_id] = 0
        AND [ep].[class] = 1
        AND [ep].[name] = N'microsoft_database_tools_support'
    )
AND [t].[name] <> '" + HistoryRepository.DefaultTableName + "'";

                if (supportsTemporalTable)
                {
                    commandText += @"
AND [t].[temporal_type] <> 1";
                }

                command.CommandText = commandText;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schema = reader.GetValueOrDefault<string>("schema");
                        var name = reader.GetValueOrDefault<string>("name");

                        if (!selectionSet.Allows(schema, name))
                        {
                            continue;
                        }

                        _logger.TableFound(DisplayName(schema, name));

                        var table = new DatabaseTable
                        {
                            Schema = schema,
                            Name = name
                        };

                        if (supportsMemoryOptimizedTable)
                        {
                            if (reader.GetValueOrDefault<bool>("is_memory_optimized"))
                            {
                                table[SqlServerAnnotationNames.MemoryOptimized] = true;
                            }
                        }

                        tables.Add(table);
                    }
                }

                // This is done separately due to MARS property may be turned off
                foreach (var table in tables)
                {
                    GetTableMetadata(connection, table, typeAliases);
                }

                return tables;
            }
        }

        private void GetTableMetadata(DbConnection connection, DatabaseTable table, IReadOnlyDictionary<string, string> typeAliases)
        {
            var name = table.Name;
            var schema = table.Schema;

            foreach (var column in GetColumns(connection, name, schema, typeAliases))
            {
                column.Table = table;
                table.Columns.Add(column);
            }

            var primaryKey = GetPrimaryKey(connection, name, schema, table.Columns);
            if (primaryKey != null)
            {
                primaryKey.Table = table;
                table.PrimaryKey = primaryKey;
            }

            foreach (var uniqueConstraints in GetUniqueConstraints(connection, name, schema, table.Columns))
            {
                uniqueConstraints.Table = table;
                table.UniqueConstraints.Add(uniqueConstraints);
            }

            foreach (var index in GetIndexes(connection, name, schema, table.Columns))
            {
                index.Table = table;
                table.Indexes.Add(index);
            }
        }

        private IEnumerable<DatabaseColumn> GetColumns(DbConnection connection, string tableName, string tableSchema, IReadOnlyDictionary<string, string> typeAliases)
        {
            using (var command = connection.CreateCommand())
            {
                var commandText = @"
SELECT
    [c].[name] AS [column_name],
    [c].[column_id] AS [ordinal],
    schema_name([tp].[schema_id]) AS [type_schema],
    [tp].[name] AS [type_name],
    CAST([c].[max_length] AS int) AS [max_length],
    CAST([c].[precision] AS int) AS [precision],
    CAST([c].[scale] AS int) AS [scale],
    [c].[is_nullable],
    [c].[is_identity],
    object_definition([c].[default_object_id]) AS [default_sql],
    [cc].[definition] AS [computed_sql]
FROM [sys].[columns] AS [c]
JOIN [sys].[types] AS [tp] ON [c].[user_type_id] = [tp].[user_type_id]
LEFT JOIN [sys].[computed_columns] AS [cc] ON [c].[object_id] = [cc].[object_id] AND [c].[column_id] = [cc].[column_id]
WHERE object_name([c].[object_id]) = @table AND object_schema_name([c].[object_id]) = @schema";

                if (Version.TryParse(connection.ServerVersion, out var serverVersion)
                    && serverVersion.Major >= 13)
                {
                    commandText += @" AND [c].[is_hidden] = 0";
                }

                commandText += @"
ORDER BY [c].[column_id]";

                command.CommandText = commandText;

                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@table";
                tableParameter.Value = tableName;

                var schemaParameter = command.CreateParameter();
                schemaParameter.ParameterName = "@schema";
                schemaParameter.Value = tableSchema;

                command.Parameters.AddRange(new[] { tableParameter, schemaParameter });

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var columnName = reader.GetValueOrDefault<string>("column_name");
                        var ordinal = reader.GetValueOrDefault<int>("ordinal");
                        var dataTypeSchemaName = reader.GetValueOrDefault<string>("type_schema");
                        var dataTypeName = reader.GetValueOrDefault<string>("type_name");
                        var maxLength = reader.GetValueOrDefault<int>("max_length");
                        var precision = reader.GetValueOrDefault<int>("precision");
                        var scale = reader.GetValueOrDefault<int>("scale");
                        var nullable = reader.GetValueOrDefault<bool>("is_nullable");
                        var isIdentity = reader.GetValueOrDefault<bool>("is_identity");
                        var defaultValue = reader.GetValueOrDefault<string>("default_sql");
                        var computedValue = reader.GetValueOrDefault<string>("computed_sql");

                        _logger.ColumnFound(
                            DisplayName(tableSchema, tableName),
                            columnName,
                            ordinal,
                            DisplayName(dataTypeSchemaName, dataTypeName),
                            maxLength,
                            precision,
                            scale,
                            nullable,
                            isIdentity,
                            defaultValue,
                            computedValue);

                        string storeType;
                        if (typeAliases.TryGetValue($"[{dataTypeSchemaName}].[{dataTypeName}]", out var underlyingStoreType))
                        {
                            storeType = dataTypeName;
                        }
                        else
                        {
                            storeType = GetStoreType(dataTypeName, maxLength, precision, scale);
                            underlyingStoreType = null;
                        }

                        if (defaultValue == "(NULL)")
                        {
                            defaultValue = null;
                        }

                        if (defaultValue == "((0))"
                            && (underlyingStoreType ?? storeType) == "bit")
                        {
                            defaultValue = null;
                        }

                        var column = new DatabaseColumn
                        {
                            Name = columnName,
                            StoreType = storeType,
                            IsNullable = nullable,
                            DefaultValueSql = defaultValue,
                            ComputedColumnSql = computedValue,
                            ValueGenerated = isIdentity
                                ? ValueGenerated.OnAdd
                                : (underlyingStoreType ?? storeType) == "rowversion"
                                    ? ValueGenerated.OnAddOrUpdate
#pragma warning disable IDE0034 // Simplify 'default' expression - Ternary expression causes default(ValueGenerated) which is non-nullable
                                    : default(ValueGenerated?)
#pragma warning restore IDE0034 // Simplify 'default' expression
                        };

                        if ((underlyingStoreType ?? storeType) == "rowversion")
                        {
                            column[ScaffoldingAnnotationNames.ConcurrencyToken] = true;
                        }

                        column.SetUnderlyingStoreType(underlyingStoreType);

                        yield return column;
                    }
                }
            }
        }

        private string GetStoreType(string dataTypeName, int maxLength, int precision, int scale)
        {
            if (dataTypeName == "timestamp")
            {
                return "rowversion";
            }

            if (dataTypeName == "decimal"
                || dataTypeName == "numeric")
            {
                return $"{dataTypeName}({precision}, {scale})";
            }

            if (_dateTimePrecisionTypes.Contains(dataTypeName)
                && scale != 7)
            {
                return $"{dataTypeName}({scale})";
            }

            if (_maxLengthRequiredTypes.Contains(dataTypeName))
            {
                if (maxLength == -1)
                {
                    return $"{dataTypeName}(max)";
                }

                if (dataTypeName == "nvarchar"
                    || dataTypeName == "nchar")
                {
                    maxLength /= 2;
                }

                return $"{dataTypeName}({maxLength})";
            }

            return dataTypeName;
        }

        private DatabasePrimaryKey GetPrimaryKey(DbConnection connection, string tableName, string tableSchema, IList<DatabaseColumn> columns)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
SELECT
    [i].[name] AS [index_name],
    [i].[type_desc],
    col_name([ic].[object_id], [ic].[column_id]) AS [column_name]
FROM [sys].[indexes] AS [i]
JOIN [sys].[index_columns] AS [ic] ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id]
WHERE object_name([i].[object_id]) = @table AND object_schema_name([i].[object_id]) = @schema AND [i].[is_primary_key] = 1
ORDER BY [ic].[key_ordinal]";

                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@table";
                tableParameter.Value = tableName;

                var schemaParameter = command.CreateParameter();
                schemaParameter.ParameterName = "@schema";
                schemaParameter.Value = tableSchema;

                command.Parameters.AddRange(new[] { tableParameter, schemaParameter });

                using (var reader = command.ExecuteReader())
                {
                    var pkGroupings = reader.Cast<DbDataRecord>()
                        .GroupBy(
                            c => (Name: c.GetValueOrDefault<string>("index_name"),
                                TypeDesc: c.GetValueOrDefault<string>("type_desc")),
                            ddr => ddr.GetValueOrDefault<string>("column_name"))
                        .ToArray();

                    if (pkGroupings.Length != 1)
                    {
                        return null;
                    }

                    var pkGroup = pkGroupings[0];

                    _logger.PrimaryKeyFound(pkGroup.Key.Name, DisplayName(tableSchema, tableName));

                    var primaryKey = new DatabasePrimaryKey
                    {
                        Name = pkGroup.Key.Name
                    };

                    if (pkGroup.Key.TypeDesc == "NONCLUSTERED")
                    {
                        primaryKey[SqlServerAnnotationNames.Clustered] = false;
                    }

                    foreach (var columnName in pkGroup)
                    {
                        var column = columns.FirstOrDefault(c => c.Name == columnName)
                                     ?? columns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                        Debug.Assert(column != null, "column is null.");

                        primaryKey.Columns.Add(column);
                    }

                    return primaryKey;
                }
            }
        }

        private IEnumerable<DatabaseUniqueConstraint> GetUniqueConstraints(DbConnection connection, string tableName, string tableSchema, IList<DatabaseColumn> columns)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
SELECT
    [i].[name] AS [index_name],
    [i].[type_desc],
    col_name([ic].[object_id], [ic].[column_id]) AS [column_name]
FROM [sys].[indexes] AS [i]
JOIN [sys].[index_columns] AS [ic] ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id]
WHERE object_name([i].[object_id]) = @table AND object_schema_name([i].[object_id]) = @schema AND [i].[is_unique_constraint] = 1
ORDER BY [index_name], [ic].[key_ordinal]";

                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@table";
                tableParameter.Value = tableName;

                var schemaParameter = command.CreateParameter();
                schemaParameter.ParameterName = "@schema";
                schemaParameter.Value = tableSchema;

                command.Parameters.AddRange(new[] { tableParameter, schemaParameter });

                using (var reader = command.ExecuteReader())
                {
                    var ucGroupings = reader.Cast<DbDataRecord>()
                        .GroupBy(
                            c => (Name: c.GetValueOrDefault<string>("index_name"),
                                TypeDesc: c.GetValueOrDefault<string>("type_desc")),
                            ddr => ddr.GetValueOrDefault<string>("column_name"))
                        .ToArray();

                    foreach (var ucGroup in ucGroupings)
                    {
                        _logger.UniqueConstraintFound(ucGroup.Key.Name, DisplayName(tableSchema, tableName));

                        var uniqueConstraint = new DatabaseUniqueConstraint
                        {
                            Name = ucGroup.Key.Name
                        };

                        if (ucGroup.Key.TypeDesc == "CLUSTERED")
                        {
                            uniqueConstraint[SqlServerAnnotationNames.Clustered] = true;
                        }

                        foreach (var columnName in ucGroup)
                        {
                            var column = columns.FirstOrDefault(c => c.Name == columnName)
                                         ?? columns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                            Debug.Assert(column != null, "column is null.");

                            uniqueConstraint.Columns.Add(column);
                        }

                        yield return uniqueConstraint;
                    }
                }
            }
        }

        private IEnumerable<DatabaseIndex> GetIndexes(DbConnection connection, string tableName, string tableSchema, IList<DatabaseColumn> columns)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
SELECT
    [i].[name] AS [index_name],
    [i].[type_desc],
	[i].[is_unique],
    [i].[has_filter],
	[i].[filter_definition],
	col_name([ic].[object_id], [ic].[column_id]) AS [column_name]
FROM [sys].[indexes] AS [i]
JOIN [sys].[index_columns] AS [ic] ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id]
WHERE object_name([i].[object_id]) = @table AND object_schema_name([i].[object_id]) = @schema AND i.is_unique_constraint <> 1 AND i.is_primary_key <> 1
ORDER BY [i].[name], [ic].[key_ordinal]";

                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@table";
                tableParameter.Value = tableName;

                var schemaParameter = command.CreateParameter();
                schemaParameter.ParameterName = "@schema";
                schemaParameter.Value = tableSchema;

                command.Parameters.AddRange(new[] { tableParameter, schemaParameter });

                using (var reader = command.ExecuteReader())
                {
                    var indexGroupings = reader.Cast<DbDataRecord>()
                        .GroupBy(
                            c => (Name: c.GetValueOrDefault<string>("index_name"),
                                TypeDesc: c.GetValueOrDefault<string>("type_desc"),
                                IsUnique: c.GetValueOrDefault<bool>("is_unique"),
                                HasFilter: c.GetValueOrDefault<bool>("has_filter"),
                                FilterDefinition: c.GetValueOrDefault<string>("filter_definition")),
                            ddr => ddr.GetValueOrDefault<string>("column_name"))
                        .ToArray();

                    foreach (var indexGroup in indexGroupings)
                    {
                        _logger.IndexFound(indexGroup.Key.Name, DisplayName(tableSchema, tableName), indexGroup.Key.IsUnique);

                        var index = new DatabaseIndex
                        {
                            Name = indexGroup.Key.Name,
                            IsUnique = indexGroup.Key.IsUnique,
                            Filter = indexGroup.Key.HasFilter ? indexGroup.Key.FilterDefinition : null
                        };

                        if (indexGroup.Key.TypeDesc == "CLUSTERED")
                        {
                            index[SqlServerAnnotationNames.Clustered] = true;
                        }

                        foreach (var columnName in indexGroup)
                        {
                            var column = columns.FirstOrDefault(c => c.Name == columnName)
                                         ?? columns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                            Debug.Assert(column != null, "column is null.");

                            index.Columns.Add(column);
                        }

                        yield return index;
                    }
                }
            }
        }

        private IEnumerable<DatabaseForeignKey> GetForeignKeys(DbConnection connection, DatabaseTable table, IList<DatabaseTable> tables)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
SELECT
    [f].[name],
	object_schema_name([f].[referenced_object_id]) AS [principal_table_schema],
    object_name([f].[referenced_object_id]) AS [principal_table_name],
	[f].[delete_referential_action_desc],
    col_name([fc].[parent_object_id], [fc].[parent_column_id]) AS [column_name],
    col_name([fc].[referenced_object_id], [fc].[referenced_column_id]) AS [referenced_column_name]
FROM [sys].[foreign_keys] AS [f]
JOIN [sys].[foreign_key_columns] AS [fc] ON [f].[object_id] = [fc].[constraint_object_id]
WHERE object_name([f].[parent_object_id]) = @table AND schema_name([f].[schema_id]) = @schema
ORDER BY [f].[name], [fc].[constraint_column_id]";

                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@table";
                tableParameter.Value = table.Name;

                var schemaParameter = command.CreateParameter();
                schemaParameter.ParameterName = "@schema";
                schemaParameter.Value = table.Schema;

                command.Parameters.AddRange(new[] { tableParameter, schemaParameter });

                using (var reader = command.ExecuteReader())
                {
                    var fkGroupings = reader.Cast<DbDataRecord>()
                        .GroupBy(
                            c => (Name: c.GetValueOrDefault<string>("name"),
                                PrincipalTableSchema: c.GetValueOrDefault<string>("principal_table_schema"),
                                PrincipalTableName: c.GetValueOrDefault<string>("principal_table_name"),
                                OnDeleteAction: c.GetValueOrDefault<string>("delete_referential_action_desc")),
                            ddr => (FkColumn: ddr.GetValueOrDefault<string>("column_name"),
                                ReferencedColumn: ddr.GetValueOrDefault<string>("referenced_column_name")))
                        .ToArray();

                    foreach (var fkGroup in fkGroupings)
                    {
                        var fkName = fkGroup.Key.Name;
                        var principalTableSchema = fkGroup.Key.PrincipalTableSchema;
                        var principalTableName = fkGroup.Key.PrincipalTableName;
                        var onDeleteAction = fkGroup.Key.OnDeleteAction;

                        _logger.ForeignKeyFound(
                            fkName,
                            DisplayName(table.Schema, table.Name),
                            DisplayName(principalTableSchema, principalTableName),
                            onDeleteAction);

                        var principalTable = tables.FirstOrDefault(
                                                 t => t.Schema == principalTableSchema
                                                      && t.Name == principalTableName)
                                             ?? tables.FirstOrDefault(
                                                 t => t.Schema.Equals(principalTableSchema, StringComparison.OrdinalIgnoreCase)
                                                      && t.Name.Equals(principalTableName, StringComparison.OrdinalIgnoreCase));

                        if (principalTable == null)
                        {
                            _logger.ForeignKeyReferencesMissingPrincipalTableWarning(
                                fkName,
                                DisplayName(table.Schema, table.Name),
                                DisplayName(principalTableSchema, principalTableName));

                            continue;
                        }

                        var foreignKey = new DatabaseForeignKey
                        {
                            Name = fkName,
                            Table = table,
                            PrincipalTable = principalTable,
                            OnDelete = ConvertToReferentialAction(onDeleteAction)
                        };

                        var invalid = false;

                        foreach (var tuple in fkGroup)
                        {
                            var columnName = tuple.FkColumn;
                            var column = table.Columns.FirstOrDefault(c => c.Name == columnName)
                                         ?? table.Columns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                            Debug.Assert(column != null, "column is null.");

                            var principalColumnName = tuple.ReferencedColumn;
                            var principalColumn = foreignKey.PrincipalTable.Columns.FirstOrDefault(c => c.Name == principalColumnName)
                                                  ?? foreignKey.PrincipalTable.Columns.FirstOrDefault(c => c.Name.Equals(principalColumnName, StringComparison.OrdinalIgnoreCase));
                            if (principalColumn == null)
                            {
                                invalid = true;
                                _logger.ForeignKeyPrincipalColumnMissingWarning(
                                    fkName,
                                    DisplayName(table.Schema, table.Name),
                                    principalColumnName,
                                    DisplayName(principalTableSchema, principalTableName));
                                break;
                            }

                            foreignKey.Columns.Add(column);
                            foreignKey.PrincipalColumns.Add(principalColumn);
                        }

                        if (!invalid)
                        {
                            yield return foreignKey;
                        }
                    }
                }
            }
        }

        private static string DisplayName(string schema, string name)
            => (!string.IsNullOrEmpty(schema) ? schema + "." : "") + name;

        private static ReferentialAction? ConvertToReferentialAction(string onDeleteAction)
        {
            switch (onDeleteAction)
            {
                case "NO_ACTION":
                    return ReferentialAction.NoAction;

                case "CASCADE":
                    return ReferentialAction.Cascade;

                case "SET_NULL":
                    return ReferentialAction.SetNull;

                case "SET_DEFAULT":
                    return ReferentialAction.SetDefault;

                default:
                    return null;
            }
        }
    }
}
