using Remotion.Linq;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Parsing.Structure;


namespace CS.QueryBuilder
{
	/// <summary>
	/// Contains command text and parameters for running a statement against a database.
	/// </summary>
	public class Command
	{
		/// <summary>
		/// Gets the statement that this command was built from.
		/// </summary>
		/// <value>
		/// The statement.
		/// </value>
		public Statement Statement { get; }

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public string CommandText { get; }

		/// <summary>
		/// Gets the parameters.
		/// </summary>
		/// <value>
		/// The parameters.
		/// </value>
		public IList<object> Parameters { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Command"/> class.
		/// </summary>
		/// <param name="statement">The statement that this command was built from.</param>
		/// <param name="commandText">The command text.</param>
		/// <param name="parameters">The parameters.</param>
		public Command(Statement statement, string commandText, object[] parameters)
		{
			this.Statement = statement;
			this.CommandText = commandText;
			this.Parameters = parameters;
		}
	}

	/// <summary>
	/// Maps .NET objects to database objects.
	/// </summary>
	public class DatabaseMapper
	{
		/// <summary>
		/// Gets the namespace in which entity classes are located.
		/// </summary>
		/// <value>
		/// The entity namespace.
		/// </value>
		public string EntityNamespace { get; set; } = "$";

		/// <summary>
		/// Gets the name of the schema for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetSchemaName(Type type)
		{
			return string.Empty;
		}

		/// <summary>
		/// Gets the name of the table for the supplied type.
		/// </summary>
		/// <remarks>
		/// For a Book item, this would return "Book" by default but might be overridden to return "Books" or something different.
		/// </remarks>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetTableName(Type type)
		{
			return type.Name;
		}

		/// <summary>
		/// Gets the name of the procedure for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetProcedureName(Type type)
		{
			return type.Name.Replace("Procedure", "");
		}

		/// <summary>
		/// Gets the name of the function for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetFunctionName(Type type)
		{
			return type.Name.Replace("Function", "");
		}

		/// <summary>
		/// Gets the name of the column for the supplied property.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetColumnName(PropertyInfo property)
		{
			return property.Name;
		}

		/// <summary>
		/// Gets the name of the primary key column.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetPrimaryKeyColumnName(Type type)
		{
			return "Id";
		}

		/// <summary>
		/// Determines whether the supplied property contains a related entity item.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns>
		///   <c>true</c> if the supplied property contains a related entity item; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsRelatedItem(PropertyInfo property)
		{
			return ShouldMapType(property.PropertyType);
		}

		/// <summary>
		/// Gets the name of the foreign key column for the supplied property.
		/// </summary>
		/// <remarks>
		/// For a Book.Author property, this would return "AuthorID" by default.
		/// </remarks>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetForeignKeyColumnName(PropertyInfo property)
		{
			return property.Name + "Id";
		}

		/// <summary>
		/// Determines whether the supplied type is a stored procedure.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a stored procedure; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsProcedure(Type type)
		{
			return type.Name.EndsWith("Procedure");
		}

		/// <summary>
		/// Determines whether the supplied type is a user-defined function.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a user-defined function; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsFunction(Type type)
		{
			return type.Name.EndsWith("Function");
		}

		/// <summary>
		/// Determines whether the class with the supplied type should be mapped to the database.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual bool ShouldMapType(Type type)
		{
			return (type.Namespace == this.EntityNamespace);
		}
	}

	/// <summary>
	/// Represents a field and aggregate (count, sum, etc) that is used with a select statement.
	/// </summary>
	public class FieldAggregate
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the aggregate (count, sum, etc).
		/// </summary>
		/// <value>
		/// The aggregate.
		/// </value>
		public AggregateType Aggregate { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldAggregate"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="aggregate">The aggregate (count, sum, etc).</param>
		public FieldAggregate(PropertyInfo field, AggregateType aggregate)
		{
			this.Field = field;
			this.Aggregate = aggregate;
		}
	}

	/// <summary>
	/// Represents a field and direction that is used for ordering a statement.
	/// </summary>
	public class FieldOrder
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the order direction (ascending or descending).
		/// </summary>
		/// <value>
		/// The direction.
		/// </value>
		public OrderDirection Direction { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldOrder"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="direction">The order direction (ascending or descending).</param>
		public FieldOrder(PropertyInfo field, OrderDirection direction = OrderDirection.Ascending)
		{
			this.Field = field;
			this.Direction = direction;
		}
	}

	/// <summary>
	/// Represents a field and the value to set it to.
	/// </summary>
	public class FieldValue
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the value to set the field to.
		/// </summary>
		/// <value>
		/// The value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldValue"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="value">The value to set the field to.</param>
		public FieldValue(PropertyInfo field, object value)
		{
			this.Field = field;
			this.Value = value;
		}
	}

	/// <summary>
	/// An interface for building command text and parameters from a statement.
	/// </summary>
	public interface ICommandBuilder
	{
		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		StringBuilder CommandText { get; }

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		List<object> ParameterValues { get; }

		/// <summary>
		/// Visits the statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		void VisitStatement(Statement statement, DatabaseMapper mapper);
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement with the name of the table that records should be selected from.
		/// </summary>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="alias">The alias.</param>
		/// <param name="schema">The schema.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(string tableName, string alias = null, string schema = null)
		{
			return Select.From(new Table(tableName, alias, schema));
		}

		/// <summary>
		/// Creates a select statement with the table that records should be selected from.
		/// </summary>
		/// <param name="table">The table.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Table table)
		{
			return new SelectStatement() { Source = table };
		}

		/// <summary>
		/// Creates a select statement from a join.
		/// </summary>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Join join)
		{
			return new SelectStatement() { Source = join };
		}

		/// <summary>
		/// Creates a select statement from a statement part.
		/// </summary>
		/// <param name="part">The part.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(StatementPart part)
		{
			return new SelectStatement() { Source = part };
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Join join)
		{
			select.SourceJoins.Add(join);
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(joinType, tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(joinType, table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params string[] columnNames)
		{
			select.SourceFields.AddRange(columnNames.Select(cn => new Column(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params SourceExpression[] columns)
		{
			select.SourceFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableNames">The table names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params string[] tableNames)
		{
			select.SourceFieldsFrom.AddRange(tableNames.Select(tn => new Table(tn)));
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tables">The tables.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params Table[] tables)
		{
			select.SourceFieldsFrom.AddRange(tables);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Count, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Count, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Sum, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Sum, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Min, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Min, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Max, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Max, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		public static SelectStatement Average(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Average, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Average(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Average, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Distinct(this SelectStatement select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="skip">The number of records to skip.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Skip(this SelectStatement select, int skip)
		{
			select.StartIndex = skip;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="take">The number of records to take.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Take(this SelectStatement select, int take)
		{
			select.Limit = take;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.AddRange(conditions);
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.And;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.Or;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(cn => new OrderByExpression(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params Column[] columns)
		{
			select.OrderByFields.AddRange(columns.Select(c => new OrderByExpression(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params OrderByExpression[] columns)
		{
			select.OrderByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order descendingly by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderByDescending(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(c => new OrderByExpression(c, OrderDirection.Descending)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params string[] columnNames)
		{
			select.GroupByFields.AddRange(columnNames.Select(c => new Column(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params Column[] columns)
		{
			select.GroupByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds another statement to the select statement as a UNION.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="union">The union.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Union(this SelectStatement select, SelectStatement union)
		{
			select.UnionStatements.Add(union);
			return select;
		}

		/// <summary>
		/// Sets additional paths to include when loading the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="path">The path.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Include(this SelectStatement select, string path)
		{
			select.IncludePaths.Add(path);
			return select;
		}
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement from a type corresponding to the table that records should be selected from.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="alias">The alias.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> From<T>(string alias = null)
		{
			return new SelectStatement<T>(alias);
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		/// <exception cref="InvalidOperationException"></exception>
		public static SelectStatement<T> Columns<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			var field = FuncToPropertyInfo(property, true);
			if (field == null)
			{
				if (property.Body is NewExpression anonObject)
				{
					// It's a new anonymous object, so add each of its arguments
					foreach (var anonArg in anonObject.Arguments)
					{
						if (anonArg is MemberExpression mex)
						{
							select.SourceFields.Add((PropertyInfo)mex.Member);
						}
					}
				}
				else
				{
					throw new InvalidOperationException();
				}
			}
			else
			{
				select.SourceFields.Add(field);
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Distinct<T>(this SelectStatement<T> select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the select statement to count records matching the supplied condition.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select.And(condition);
		}

		/// <summary>
		/// Sets the select statement to count all records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select;
		}

		/// <summary>
		/// Sets the select statement to sum the supplied properties.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Sum<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.AggregateFields.Add(new FieldAggregate(FuncToPropertyInfo(property), AggregateType.Sum));
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="startIndex">The start index.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Skip<T>(this SelectStatement<T> select, int startIndex)
		{
			select.StartIndex = startIndex;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="limit">The limit.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Take<T>(this SelectStatement<T> select, int limit)
		{
			select.Limit = limit;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Where<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			return select.And(condition);
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> And<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.AndAlso(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Or<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.OrElse(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to group by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> GroupBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.GroupByFields.Add(FuncToPropertyInfo(property));
			return select;
		}

		private static PropertyInfo FuncToPropertyInfo<T>(Expression<Func<T, object>> selector, bool returnNull = false)
		{
			if (selector.Body is MemberExpression mex)
			{
				return (PropertyInfo)mex.Member;
			}
			else if (selector.Body is UnaryExpression uex)
			{
				// Throw away converts
				if (uex.Operand is MemberExpression omex)
				{
					return (PropertyInfo)omex.Member;
				}
			}

			// HACK: Yes, this is ugly!
			if (returnNull)
			{
				return null;
			}
			else
			{
				throw new InvalidOperationException();
			}
		}
	}

	/// <summary>
	/// Builds command text and parameters from a statement for use in an SQL database.
	/// </summary>
	/// <seealso cref="CS.QueryBuilder.ICommandBuilder" />
	public class SqlCommandBuilder : ICommandBuilder
	{
		private const int IndentationWidth = 2;

		private enum Indentation
		{
			Same,
			Inner,
			Outer
		}

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public StringBuilder CommandText { get; } = new StringBuilder();

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		public List<object> ParameterValues { get; } = new List<object>();

		private int Depth { get; set; }

		private bool IsNested { get; set; }

		/// <summary>
		/// Visits a statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		/// <exception cref="NotSupportedException"></exception>
		public void VisitStatement(Statement statement, DatabaseMapper mapper)
		{
			switch (statement.PartType)
			{
				case StatementPartType.Select:
				{
					VisitSelect((SelectStatement)statement);
					break;
				}
				case StatementPartType.GenericSelect:
				{
					var select = (SelectStatement)((GenericStatement)statement).CreateStatement(mapper);
					VisitSelect(select);
					break;
				}

				default:
				{
					// TODO:
					throw new NotSupportedException();
				}
			}
		}

		/// <summary>
		/// Visits a constant part.
		/// </summary>
		/// <param name="constant">The constant part.</param>
		protected virtual void VisitConstant(ConstantPart constant)
		{
			VisitObject(constant.Value);
			if (!string.IsNullOrEmpty(constant.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(constant.Alias);
				this.CommandText.Append("]");
			}
		}

		/// <summary>
		/// Visits an object.
		/// </summary>
		/// <param name="value">The object.</param>
		protected virtual void VisitObject(object value)
		{
			if (value == null)
			{
				this.CommandText.Append("NULL");
			}
			else if (value.GetType() == typeof(bool))
			{
				this.CommandText.Append(((bool)value) ? "1" : "0");
			}
			else if (value.GetType() == typeof(string) && value.ToString().Length == 0)
			{
				this.CommandText.Append("''");
			}
			else if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
			{
				var firstValue = true;
				foreach (var innerValue in enumerable)
				{
					if (!firstValue)
					{
						this.CommandText.Append(", ");
					}
					firstValue = false;
					if (innerValue is ConstantPart constantValue)
					{
						this.VisitConstant(constantValue);
					}
					else
					{
						this.VisitObject(innerValue);
					}
				}
			}
			else
			{
				var index = this.ParameterValues.IndexOf(value);
				if (index != -1)
				{
					this.CommandText.Append("@");
					this.CommandText.Append(index);
				}
				else
				{
					this.CommandText.Append("@");
					this.CommandText.Append(this.ParameterValues.Count);
					if (value.GetType().IsEnum)
					{
						this.ParameterValues.Add(Convert.ToInt64(value));
					}
					else
					{
						this.ParameterValues.Add(value);
					}
				}
			}
		}

		/// <summary>
		/// Visits a select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelect(SelectStatement select)
		{
			// TODO: If we're using SQL Server 2012 we should just use the OFFSET keyword
			if (select.StartIndex > 0)
			{
				VisitSelectWithRowNumber(select);
				return;
			}

			if (select.IsAny)
			{
				VisitSelectWithAny(select);
				return;
			}

			if (select.IsAll)
			{
				VisitSelectWithAll(select);
				return;
			}

			if (select.IsContains)
			{
				VisitSelectWithContains(select);
				return;
			}

			// If any of the fields have aggregates that aren't grouped, remove the ordering as SQL Server doesn't like it
			// TODO: Only if they aren't grouped
			if (select.SourceFields.Any(f => f is Aggregate))
			{
				select.OrderByFields.Clear();
			}

			this.CommandText.Append("SELECT ");
			if (select.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and remove this method
				VisitLimitAtStart(select);
			}
			if (select.SourceFieldsFrom.Count > 0)
			{
				VisitSourceFieldsFrom(select);
			}
			if (select.SourceFields.Count > 0)
			{
				VisitSourceFields(select);
			}
			if (select.SourceFieldsFrom.Count == 0 && select.SourceFields.Count == 0)
			{
				if (this.IsNested)
				{
					// TODO: Rename tmp, it sucks
					this.CommandText.Append("NULL ");
					this.CommandText.Append("AS tmp");
				}
				else
				{
					// TODO: When to use "*" vs "NULL"?
					this.CommandText.Append("*");
				}
			}
			if (select.Source != null)
			{
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append("FROM ");
				this.VisitSource(select.Source);
			}
			if (select.SourceJoins != null)
			{
				for (var i = 0; i < select.SourceJoins.Count; i++)
				{
					this.AppendNewLine(Indentation.Same);
					this.VisitJoin(select.SourceJoins[i]);
				}
			}
			if (select.Conditions.Count > 0)
			{
				VisitConditions(select.Conditions);
			}
			if (select.GroupByFields.Count > 0)
			{
				VisitGroupBy(select);
			}
			if (select.OrderByFields.Count > 0 && !select.IsAggregate)
			{
				VisitOrderBy(select);
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and rename this method
				VisitLimitAtEnd(select);
			}
			foreach (var union in select.UnionStatements)
			{
				this.CommandText.AppendLine();
				this.CommandText.AppendLine("UNION ALL");
				VisitSelect(union);
			}
		}

		/// <summary>
		/// Visits a select statement with a row number.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithRowNumber(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT Fields
			// FROM (SELECT Fields,
			//		ROW_NUMBER() OVER (ORDER BY OrderFields) AS RowNumber
			//		FROM Table
			//		WHERE Condition)
			// WHERE RowNumber > Start AND RowNumber <= End
			// ORDER BY OrderFields

			// Clone the select and add the RowNumber field to it
			var inner = Select.From(select.Source);
			inner.SourceJoins.AddRange(select.SourceJoins);
			inner.Alias = "RowNumberTable";
			inner.SourceFields.AddRange(select.SourceFields);
			inner.SourceFields.Add(new RowNumber(select.OrderByFields.ToArray()));
			inner.Conditions.AddRange(select.Conditions);

			// If the original table selected all fields, we need to add another field to select them ourselves
			if (!select.SourceFields.Any())
			{
				if (select.Source is Table table)
				{
					inner.SourceFields.Add(new Column(table.Name, "*"));
				}
			}

			// Clone the select and change its source
			var outer = Select.From(inner);
			foreach (var field in select.SourceFields)
			{
				if (field is Column column)
				{
					outer.SourceFields.Add(new Column(inner.Alias, column.Name));
				}
			}
			if (select.StartIndex > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsGreaterThan, select.StartIndex));
			}
			if (select.Limit > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsLessThanOrEqualTo, select.StartIndex + select.Limit));
			}
			outer.OrderByFields.Add(new OrderByExpression("RowNumber"));

			// Visit the outer select
			VisitSelect(outer);
		}

		/// <summary>
		/// Visits a select statement with ANY.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAny(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN EXISTS (");
			this.Indent(Indentation.Inner);

			select.IsAny = false;

			this.VisitSelect(select);

			select.IsAny = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with ALL.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAll(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN NOT EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE NOT Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN NOT EXISTS (");
			this.Indent(Indentation.Inner);

			var not = select.Conditions.Not;
			select.IsAll = false;
			select.Conditions.Not = !not;

			this.VisitSelect(select);

			select.IsAll = true;
			select.Conditions.Not = not;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with CONTAINS.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithContains(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN @0 IN (
			//		SELECT Fields
			//		FROM Table
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN ");
			this.VisitField(select.ContainsItem);
			this.CommandText.Append(" IN (");
			this.Indent(Indentation.Inner);

			select.IsContains = false;

			this.VisitSelect(select);

			select.IsContains = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a source fields.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFields(SelectStatement select)
		{
			for (var i = 0; i < select.SourceFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.SourceFields[i]);
			}
		}

		/// <summary>
		/// Visits a source fields from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFieldsFrom(SelectStatement select)
		{
			// TODO: Should the SourceFieldsFrom actually be its own class?
			for (var i = 0; i < select.SourceFieldsFrom.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitTable(select.SourceFieldsFrom[i]);
				this.CommandText.Append(".*");
			}
			if (select.SourceFields.Count > 0)
			{
				this.CommandText.Append(", ");
			}
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="conditions">The condition collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditions(ConditionCollection conditions)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("WHERE ");
			if (conditions.Not)
			{
				this.CommandText.Append("NOT ");
			}
			for (var i = 0; i < conditions.Count; i++)
			{
				if (i > 0)
				{
					this.AppendNewLine(Indentation.Same);
					switch (conditions[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(conditions[i]);
			}
		}

		/// <summary>
		/// Visits a group by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitGroupBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("GROUP BY ");
			for (var i = 0; i < select.GroupByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.GroupByFields[i]);
			}
		}

		/// <summary>
		/// Visits an order by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitOrderBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("ORDER BY ");
			for (var i = 0; i < select.OrderByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.OrderByFields[i].Expression);
				if (select.OrderByFields[i].Direction != OrderDirection.Ascending)
				{
					this.CommandText.Append(" DESC");
				}
			}
		}

		/// <summary>
		/// Visits a limit at start.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtStart(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a limit at end.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtEnd(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a field.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitField(StatementPart field)
		{
			switch (field.PartType)
			{
				case StatementPartType.Column:
				{
					this.VisitColumn((Column)field);
					break;
				}
				case StatementPartType.RowNumber:
				{
					this.VisitRowNumber((RowNumber)field);
					break;
				}
				case StatementPartType.Aggregate:
				{
					this.VisitAggregate((Aggregate)field);
					break;
				}
				case StatementPartType.ConditionalCase:
				{
					this.VisitConditionalCase((ConditionalCase)field);
					break;
				}
				case StatementPartType.ConditionPredicate:
				{
					this.VisitConditionPredicate((ConditionPredicate)field);
					break;
				}
				case StatementPartType.Exists:
				{
					this.VisitExists((Exists)field);
					break;
				}
				case StatementPartType.CoalesceFunction:
				{
					this.VisitCoalesceFunction((CoalesceFunction)field);
					break;
				}
				case StatementPartType.ConvertFunction:
				{
					this.VisitConvertFunction((ConvertFunction)field);
					break;
				}
				case StatementPartType.StringLengthFunction:
				{
					this.VisitStringLengthFunction((StringLengthFunction)field);
					break;
				}
				case StatementPartType.SubstringFunction:
				{
					this.VisitSubstringFunction((SubstringFunction)field);
					break;
				}
				case StatementPartType.StringRemoveFunction:
				{
					this.VisitStringRemoveFunction((StringRemoveFunction)field);
					break;
				}
				case StatementPartType.StringIndexFunction:
				{
					this.VisitStringCharIndexFunction((StringIndexFunction)field);
					break;
				}
				case StatementPartType.StringToUpperFunction:
				{
					this.VisitStringToUpperFunction((StringToUpperFunction)field);
					break;
				}
				case StatementPartType.StringToLowerFunction:
				{
					this.VisitStringToLowerFunction((StringToLowerFunction)field);
					break;
				}
				case StatementPartType.StringReplaceFunction:
				{
					this.VisitStringReplaceFunction((StringReplaceFunction)field);
					break;
				}
				case StatementPartType.StringTrimFunction:
				{
					this.VisitStringTrimFunction((StringTrimFunction)field);
					break;
				}
				case StatementPartType.StringCompareFunction:
				{
					this.VisitStringCompareFunction((StringCompareFunction)field);
					break;
				}
				case StatementPartType.StringConcatenateFunction:
				{
					this.VisitStringConcatenateFunction((StringConcatenateFunction)field);
					break;
				}
				case StatementPartType.DatePartFunction:
				{
					this.VisitDatePartFunction((DatePartFunction)field);
					break;
				}
				case StatementPartType.DateAddFunction:
				{
					this.VisitDateAddFunction((DateAddFunction)field);
					break;
				}
				case StatementPartType.DateNewFunction:
				{
					this.VisitDateNewFunction((DateNewFunction)field);
					break;
				}
				case StatementPartType.DateDifferenceFunction:
				{
					this.VisitDateDifferenceFunction((DateDifferenceFunction)field);
					break;
				}
				case StatementPartType.NumberAbsoluteFunction:
				{
					this.VisitNumberAbsoluteFunction((NumberAbsoluteFunction)field);
					break;
				}
				case StatementPartType.NumberNegateFunction:
				{
					this.VisitNumberNegateFunction((NumberNegateFunction)field);
					break;
				}
				case StatementPartType.NumberCeilingFunction:
				{
					this.VisitNumberCeilingFunction((NumberCeilingFunction)field);
					break;
				}
				case StatementPartType.NumberFloorFunction:
				{
					this.VisitNumberFloorFunction((NumberFloorFunction)field);
					break;
				}
				case StatementPartType.NumberRoundFunction:
				{
					this.VisitNumberRoundFunction((NumberRoundFunction)field);
					break;
				}
				case StatementPartType.NumberTruncateFunction:
				{
					this.VisitNumberTruncateFunction((NumberTruncateFunction)field);
					break;
				}
				case StatementPartType.NumberSignFunction:
				{
					this.VisitNumberSignFunction((NumberSignFunction)field);
					break;
				}
				case StatementPartType.NumberPowerFunction:
				{
					this.VisitNumberPowerFunction((NumberPowerFunction)field);
					break;
				}
				case StatementPartType.NumberRootFunction:
				{
					this.VisitNumberRootFunction((NumberRootFunction)field);
					break;
				}
				case StatementPartType.NumberExponentialFunction:
				{
					this.VisitNumberExponentialFunction((NumberExponentialFunction)field);
					break;
				}
				case StatementPartType.NumberLogFunction:
				{
					this.VisitNumberLogFunction((NumberLogFunction)field);
					break;
				}
				case StatementPartType.NumberLog10Function:
				{
					this.VisitNumberLog10Function((NumberLog10Function)field);
					break;
				}
				case StatementPartType.NumberTrigFunction:
				{
					this.VisitNumberTrigFunction((NumberTrigFunction)field);
					break;
				}
				case StatementPartType.BinaryOperation:
				{
					this.VisitBinaryOperation((BinaryOperation)field);
					break;
				}
				case StatementPartType.UnaryOperation:
				{
					this.VisitUnaryOperation((UnaryOperation)field);
					break;
				}
				case StatementPartType.LiteralPart:
				{
					this.VisitLiteralPart((LiteralPart)field);
					break;
				}
				case StatementPartType.Select:
				{
					this.VisitSelect((SelectStatement)field);
					break;
				}
				case StatementPartType.ConstantPart:
				{
					this.VisitConstant((ConstantPart)field);
					break;
				}
				case StatementPartType.Condition:
				{
					this.VisitCondition((Condition)field);
					break;
				}
				case StatementPartType.FieldCollection:
				{
					var collection = (FieldCollection)field;
					for (var i = 0; i < collection.Count; i++)
					{
						if (i > 0)
						{
							this.CommandText.Append(", ");
						}
						this.VisitField(collection[i]);
					}
					break;
				}
				case StatementPartType.SelectExpression:
				{
					this.VisitSelectExpression((SelectExpression)field);
					break;
				}
				default:
				{
					// TODO: Words for all exceptions
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a column.
		/// </summary>
		/// <param name="column">The column.</param>
		/// <param name="ignoreTablePrefix">if set to <c>true</c> [ignore table prefix].</param>
		protected virtual void VisitColumn(Column column, bool ignoreTablePrefix = false)
		{
			if (!ignoreTablePrefix && column.Table != null && !string.IsNullOrEmpty(column.Table.Name))
			{
				if (!string.IsNullOrEmpty(column.Table.Alias))
				{
					this.CommandText.Append("[");
					this.CommandText.Append(column.Table.Alias);
					this.CommandText.Append("]");
				}
				else
				{
					VisitTable(column.Table);
				}
				this.CommandText.Append(".");
			}

			if (column.Name == "*")
			{
				this.CommandText.Append("*");
			}
			else if (column.Name.StartsWith("@"))
			{
				// HACK: Allowing the user to pass parameter names in with new Column("@ParameterID")
				// but it might be better to require new Parameter("@ParameterID")
				this.CommandText.Append(column.Name);
			}
			else
			{
				this.CommandText.Append("[");
				this.CommandText.Append(column.Name);
				this.CommandText.Append("]");
				if (!string.IsNullOrEmpty(column.Alias))
				{
					this.CommandText.Append(" AS [");
					this.CommandText.Append(column.Alias);
					this.CommandText.Append("]");
				}
			}
		}

		/// <summary>
		/// Visits a source.
		/// </summary>
		/// <param name="source">The source.</param>
		/// <exception cref="InvalidOperationException">Select source is not valid type</exception>
		protected virtual void VisitSource(StatementPart source)
		{
			var previousIsNested = this.IsNested;
			this.IsNested = true;
			switch (source.PartType)
			{
				case StatementPartType.Table:
				{
					var table = (Table)source;
					this.VisitTable(table);
					if (!string.IsNullOrEmpty(table.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(table.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				case StatementPartType.Select:
				{
					var select = (SelectStatement)source;
					this.CommandText.Append("(");
					this.AppendNewLine(Indentation.Inner);
					this.VisitSelect(select);
					this.AppendNewLine(Indentation.Same);
					this.CommandText.Append(")");
					if (!string.IsNullOrEmpty(select.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(select.Alias);
						this.CommandText.Append("]");
					}
					this.Indent(Indentation.Outer);
					break;
				}
				case StatementPartType.Join:
				{
					this.VisitJoin((Join)source);
					break;
				}
				case StatementPartType.UserDefinedFunction:
				{
					var function = (UserDefinedFunction)source;
					this.VisitUserDefinedFunction(function);
					if (!string.IsNullOrEmpty(function.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(function.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				default:
				{
					throw new InvalidOperationException("Select source is not valid type");
				}
			}
			this.IsNested = previousIsNested;
		}

		/// <summary>
		/// Visits a table.
		/// </summary>
		/// <param name="table">The table.</param>
		protected virtual void VisitTable(Table table)
		{
			if (!string.IsNullOrEmpty(table.Schema))
			{
				this.CommandText.Append("[");
				this.CommandText.Append(table.Schema);
				this.CommandText.Append("]");
				this.CommandText.Append(".");
			}
			this.CommandText.Append("[");
			this.CommandText.Append(table.Name);
			this.CommandText.Append("]");
		}

		/// <summary>
		/// Visits an user defined function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitUserDefinedFunction(UserDefinedFunction function)
		{
			if (!string.IsNullOrEmpty(function.Schema))
			{
				this.CommandText.Append(function.Schema);
				this.CommandText.Append(".");
			}
			this.CommandText.Append(function.Name);
			this.CommandText.Append("(");
			for (var i = 0; i < function.Parameters.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitObject(function.Parameters[i].Value);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a join.
		/// </summary>
		/// <param name="join">The join.</param>
		protected virtual void VisitJoin(Join join)
		{
			switch (join.JoinType)
			{
				case JoinType.Inner:
				{
					this.CommandText.Append("INNER JOIN ");
					break;
				}
				case JoinType.Left:
				{
					this.CommandText.Append("LEFT OUTER JOIN ");
					break;
				}
				case JoinType.Right:
				{
					this.CommandText.Append("RIGHT OUTER JOIN ");
					break;
				}
				case JoinType.Cross:
				{
					this.CommandText.Append("CROSS JOIN ");
					break;
				}
				case JoinType.CrossApply:
				{
					this.CommandText.Append("CROSS APPLY ");
					break;
				}
			}
			this.VisitSource(join.Table);
			if (join.Conditions.Count > 0)
			{
				this.CommandText.Append(" ON ");
				this.VisitConditionCollection(join.Conditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitCondition(ConditionExpression condition)
		{
			// TODO: Should all types of conditions be a class?  Not exposed to the user, because that
			// interface would be gross
			if (condition is Exists existsCondition)
			{
				VisitExists(existsCondition);
				return;
			}

			if (condition.Not)
			{
				this.CommandText.Append("NOT ");
			}

			if (condition is Condition singleCondition)
			{
				VisitCondition(singleCondition);
			}
			else if (condition is ConditionCollection multipleConditions)
			{
				VisitConditionCollection(multipleConditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		/// <exception cref="InvalidOperationException">Invalid operator: " + condition.Operator</exception>
		protected virtual void VisitCondition(Condition condition)
		{
			// Check for null comparisons first
			var fieldIsNull = (condition.Field is ConstantPart constantField && constantField.Value == null);
			var valueIsNull = (condition.Value is ConstantPart constantValue && constantValue.Value == null);
			if ((condition.Operator == SqlOperator.Equals || condition.Operator == SqlOperator.NotEquals) &&
				(fieldIsNull || valueIsNull))
			{
				if (fieldIsNull)
				{
					this.VisitField(condition.Value);
				}
				else if (valueIsNull)
				{
					this.VisitField(condition.Field);
				}
				if (condition.Operator == SqlOperator.Equals)
				{
					this.CommandText.Append(" IS NULL");
				}
				else if (condition.Operator == SqlOperator.NotEquals)
				{
					this.CommandText.Append(" IS NOT NULL");
				}
			}
			else
			{
				switch (condition.Operator)
				{
					case SqlOperator.Equals:
					{
						VisitEqualsCondition(condition);
						break;
					}
					case SqlOperator.NotEquals:
					{
						VisitNotEqualsCondition(condition);
						break;
					}
					case SqlOperator.IsLessThan:
					{
						VisitIsLessThanCondition(condition);
						break;
					}
					case SqlOperator.IsLessThanOrEqualTo:
					{
						VisitIsLessThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThan:
					{
						VisitIsGreaterThanCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThanOrEqualTo:
					{
						VisitIsGreaterThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsIn:
					{
						VisitIsInCondition(condition);
						break;
					}
					case SqlOperator.Contains:
					{
						VisitContainsCondition(condition);
						break;
					}
					case SqlOperator.StartsWith:
					{
						VisitStartsWithCondition(condition);
						break;
					}
					case SqlOperator.EndsWith:
					{
						VisitEndsWithCondition(condition);
						break;
					}
					default:
					{
						throw new InvalidOperationException("Invalid operator: " + condition.Operator);
					}
				}
			}
		}

		/// <summary>
		/// Visits an equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" = ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a not equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitNotEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <> ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" < ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" > ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" >= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is in condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsInCondition(Condition condition)
		{
			// If it's in an empty list, just check against false
			var handled = false;
			if (condition.Value.PartType == StatementPartType.ConstantPart)
			{
				var value = ((ConstantPart)condition.Value).Value;
				if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
				{
					// HACK: Ugh
					var hasThings = false;
					foreach (var thing in enumerable)
					{
						hasThings = true;
						break;
					}
					if (!hasThings)
					{
						handled = true;
						this.CommandText.Append(" 0 <> 0");
					}
				}
			}
			if (!handled)
			{
				this.VisitField(condition.Field);
				this.CommandText.Append(" IN (");
				this.AppendNewLine(Indentation.Inner);
				this.VisitField(condition.Value);
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append(")");
				this.AppendNewLine(Indentation.Outer);
			}
		}

		/// <summary>
		/// Visits a contains condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitContainsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits a starts with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitStartsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits an ends with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEndsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="collection">The collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditionCollection(ConditionCollection collection)
		{
			this.CommandText.Append("(");
			for (var i = 0; i < collection.Count; i++)
			{
				if (i > 0)
				{
					// TODO: make this a visitrelationship method
					this.AppendNewLine(Indentation.Same);
					switch (collection[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(collection[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a conditional case.
		/// </summary>
		/// <param name="conditional">The conditional.</param>
		protected virtual void VisitConditionalCase(ConditionalCase conditional)
		{
			if (conditional.Test is Condition)
			{
				this.CommandText.Append("(CASE WHEN ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" THEN ");
				this.VisitField(conditional.IfTrue);
				var ifFalse = conditional.IfFalse;
				while (ifFalse != null && ifFalse.PartType == StatementPartType.ConditionalCase)
				{
					var subconditional = (ConditionalCase)conditional.IfFalse;
					this.CommandText.Append(" WHEN ");
					this.VisitField(subconditional.Test);
					this.CommandText.Append(" THEN ");
					this.VisitField(subconditional.IfTrue);
					ifFalse = subconditional.IfFalse;
				}
				if (ifFalse != null)
				{
					this.CommandText.Append(" ELSE ");
					this.VisitField(ifFalse);
				}
				this.CommandText.Append(" END)");
			}
			else
			{
				this.CommandText.Append("(CASE ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" WHEN 0 THEN ");
				this.VisitField(conditional.IfFalse);
				this.CommandText.Append(" ELSE ");
				this.VisitField(conditional.IfTrue);
				this.CommandText.Append(" END)");
			}
		}

		/// <summary>
		/// Visits a row number.
		/// </summary>
		/// <param name="rowNumber">The row number.</param>
		protected virtual void VisitRowNumber(RowNumber rowNumber)
		{
			this.CommandText.Append("ROW_NUMBER() OVER(");
			if (rowNumber.OrderByFields != null && rowNumber.OrderByFields.Count > 0)
			{
				this.CommandText.Append("ORDER BY ");
				for (var i = 0; i < rowNumber.OrderByFields.Count; i++)
				{
					if (i > 0)
					{
						this.CommandText.Append(", ");
					}
					this.VisitField(rowNumber.OrderByFields[i].Expression);
					if (rowNumber.OrderByFields[i].Direction != OrderDirection.Ascending)
					{
						this.CommandText.Append(" DESC");
					}
				}
			}
			this.CommandText.Append(") AS RowNumber");
		}

		/// <summary>
		/// Visits an aggregate.
		/// </summary>
		/// <param name="aggregate">The aggregate.</param>
		protected virtual void VisitAggregate(Aggregate aggregate)
		{
			this.CommandText.Append(GetAggregateName(aggregate.AggregateType));
			this.CommandText.Append("(");
			if (aggregate.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (aggregate.Field != null)
			{
				this.VisitField(aggregate.Field);
			}
			else if (aggregate.AggregateType == AggregateType.Count ||
				aggregate.AggregateType == AggregateType.BigCount)
			{
				this.CommandText.Append("*");
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Gets the name of the aggregate.
		/// </summary>
		/// <param name="aggregateType">Type of the aggregate.</param>
		/// <returns></returns>
		/// <exception cref="Exception">Unknown aggregate type: {aggregateType}</exception>
		private string GetAggregateName(AggregateType aggregateType)
		{
			switch (aggregateType)
			{
				case AggregateType.Count:
				{
					return "COUNT";
				}
				case AggregateType.BigCount:
				{
					return "COUNT_BIG";
				}
				case AggregateType.Min:
				{
					return "MIN";
				}
				case AggregateType.Max:
				{
					return "MAX";
				}
				case AggregateType.Sum:
				{
					return "SUM";
				}
				case AggregateType.Average:
				{
					return "AVG";
				}
				default:
				{
					throw new Exception($"Unknown aggregate type: {aggregateType}");
				}
			}
		}

		/// <summary>
		/// Visits a condition predicate.
		/// </summary>
		/// <param name="predicate">The predicate.</param>
		protected virtual void VisitConditionPredicate(ConditionPredicate predicate)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(predicate.Predicate);
			this.CommandText.Append(" THEN 1 ELSE 0 END)");
		}

		/// <summary>
		/// Visits an exists.
		/// </summary>
		/// <param name="exists">The exists.</param>
		protected virtual void VisitExists(Exists exists)
		{
			if (exists.Not)
			{
				this.CommandText.Append("NOT ");
			}
			this.CommandText.Append("EXISTS (");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(exists.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			this.Indent(Indentation.Outer);
		}

		/// <summary>
		/// Visits a coalesce function.
		/// </summary>
		/// <param name="coalesce">The coalesce.</param>
		protected virtual void VisitCoalesceFunction(CoalesceFunction coalesce)
		{
			StatementPart first = coalesce.Arguments[0];
			StatementPart second = coalesce.Arguments[1];

			this.CommandText.Append("COALESCE(");
			this.VisitField(first);
			this.CommandText.Append(", ");
			while (second.PartType == StatementPartType.CoalesceFunction)
			{
				var secondCoalesce = (CoalesceFunction)second;
				this.VisitField(secondCoalesce.Arguments[0]);
				this.CommandText.Append(", ");
				second = secondCoalesce.Arguments[1];
			}
			this.VisitField(second);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a function.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="arguments">The arguments.</param>
		protected virtual void VisitFunction(string name, params StatementPart[] arguments)
		{
			this.CommandText.Append(name);
			this.CommandText.Append("(");
			for (var i = 0; i < arguments.Length; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(arguments[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a convert function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitConvertFunction(ConvertFunction function)
		{
			// TODO: Handle more types
			this.CommandText.Append("CONVERT(VARCHAR, ");
			this.VisitField(function.Expression);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string length function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringLengthFunction(StringLengthFunction function)
		{
			VisitFunction("LEN", function.Argument);
		}

		/// <summary>
		/// Visits a substring function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitSubstringFunction(SubstringFunction function)
		{
			this.CommandText.Append("SUBSTRING(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string remove function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringRemoveFunction(StringRemoveFunction function)
		{
			this.CommandText.Append("STUFF(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(", '')");
		}

		/// <summary>
		/// Visits a string character index function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCharIndexFunction(StringIndexFunction function)
		{
			this.CommandText.Append("(");
			if (function.StartIndex != null)
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument, function.StartIndex);
			}
			else
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument);
			}
			this.CommandText.Append(" - 1)");
		}

		/// <summary>
		/// Visits a string to upper function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToUpperFunction(StringToUpperFunction function)
		{
			VisitFunction("UPPER", function.Argument);
		}

		/// <summary>
		/// Visits a string to lower function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToLowerFunction(StringToLowerFunction function)
		{
			VisitFunction("LOWER", function.Argument);
		}

		/// <summary>
		/// Visits a string replace function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringReplaceFunction(StringReplaceFunction function)
		{
			VisitFunction("REPLACE", function.Argument, function.OldValue, function.NewValue);
		}

		/// <summary>
		/// Visits a string trim function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringTrimFunction(StringTrimFunction function)
		{
			this.CommandText.Append("RTRIM(LTRIM(");
			this.VisitField(function.Argument);
			this.CommandText.Append("))");
		}

		/// <summary>
		/// Visits a string compare function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCompareFunction(StringCompareFunction function)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" = ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN 0 WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" < ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN -1 ELSE 1 END)");
		}

		/// <summary>
		/// Visits a string concatenate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringConcatenateFunction(StringConcatenateFunction function)
		{
			for (var i = 0; i < function.Arguments.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(" + ");
				}
				this.VisitField(function.Arguments[i]);
			}
		}

		/// <summary>
		/// Visits a date part function.
		/// </summary>
		/// <param name="function">The function.</param>
		/// <exception cref="InvalidOperationException">Invalid date part: " + function.DatePart</exception>
		protected virtual void VisitDatePartFunction(DatePartFunction function)
		{
			switch (function.DatePart)
			{
				case DatePart.Millisecond:
				case DatePart.Second:
				case DatePart.Minute:
				case DatePart.Hour:
				case DatePart.Day:
				case DatePart.Month:
				case DatePart.Year:
				{
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
						function.Argument
					});
					break;
				}
				case DatePart.DayOfWeek:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("weekday"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.DayOfYear:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("dayofyear"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.Date:
				{
					this.CommandText.Append("DATEADD(dd, DATEDIFF(dd, 0, ");
					this.VisitField(function.Argument);
					this.CommandText.Append("), 0)");
					break;
				}
				default:
				{
					throw new InvalidOperationException("Invalid date part: " + function.DatePart);
				}
			}
		}

		/// <summary>
		/// Visits a date add function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateAddFunction(DateAddFunction function)
		{
			this.VisitFunction("DATEADD", new StatementPart[]
			{
				new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
				function.Number,
				function.Argument
			});
		}

		/// <summary>
		/// Visits a date new function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateNewFunction(DateNewFunction function)
		{
			if (function.Hour != null)
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append(") + ' ' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Hour);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Minute);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Second);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append("))");
			}

		}

		/// <summary>
		/// Visits a date difference function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateDifferenceFunction(DateDifferenceFunction function)
		{
			this.VisitFunction("DATEDIFF", function.Date1, function.Date2);
		}

		/// <summary>
		/// Visits a number absolute function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberAbsoluteFunction(NumberAbsoluteFunction function)
		{
			this.VisitFunction("ABS", function.Argument);
		}

		/// <summary>
		/// Visits a number negate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberNegateFunction(NumberNegateFunction function)
		{
			this.CommandText.Append("-");
			this.VisitField(function.Argument);
		}

		/// <summary>
		/// Visits a number ceiling function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberCeilingFunction(NumberCeilingFunction function)
		{
			this.VisitFunction("CEILING", function.Argument);
		}

		/// <summary>
		/// Visits a number floor function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberFloorFunction(NumberFloorFunction function)
		{
			this.VisitFunction("FLOOR", function.Argument);
		}

		/// <summary>
		/// Visits a number round function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRoundFunction(NumberRoundFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, function.Precision);
		}

		/// <summary>
		/// Visits a number truncate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTruncateFunction(NumberTruncateFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, new ConstantPart(0), new ConstantPart(1));
		}

		/// <summary>
		/// Visits a number sign function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberSignFunction(NumberSignFunction function)
		{
			this.VisitFunction("SIGN", function.Argument);
		}

		/// <summary>
		/// Visits a number power function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberPowerFunction(NumberPowerFunction function)
		{
			this.VisitFunction("POWER", function.Argument, function.Power);
		}

		/// <summary>
		/// Visits a number root function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRootFunction(NumberRootFunction function)
		{
			// TODO: I'm being lazy, if root > 3 then we should to convert it to POW(argument, 1 / root)
			this.VisitFunction("SQRT", function.Argument);
		}

		/// <summary>
		/// Visits a number exponential function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberExponentialFunction(NumberExponentialFunction function)
		{
			this.VisitFunction("EXP", function.Argument);
		}

		/// <summary>
		/// Visits a number log function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLogFunction(NumberLogFunction function)
		{
			this.VisitFunction("LOG", function.Argument);
		}

		/// <summary>
		/// Visits a number log10 function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLog10Function(NumberLog10Function function)
		{
			this.VisitFunction("LOG10", function.Argument);
		}

		/// <summary>
		/// Visits a number trig function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTrigFunction(NumberTrigFunction function)
		{
			if (function.Argument2 != null)
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument, function.Argument2);
			}
			else
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument);
			}
		}

		/// <summary>
		/// Visits a binary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitBinaryOperation(BinaryOperation operation)
		{
			if (operation.Operator == BinaryOperator.LeftShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" * POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else if (operation.Operator == BinaryOperator.RightShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" / POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" ");
				this.CommandText.Append(GetOperatorName(operation.Operator));
				this.CommandText.Append(" ");
				this.VisitField(operation.Right);
				this.CommandText.Append(")");
			}
		}

		private string GetOperatorName(BinaryOperator op)
		{
			switch (op)
			{
				case BinaryOperator.Add:
				{
					return "+";
				}
				case BinaryOperator.Subtract:
				{
					return "-";
				}
				case BinaryOperator.Multiply:
				{
					return "*";
				}
				case BinaryOperator.Divide:
				{
					return "/";
				}
				case BinaryOperator.Remainder:
				{
					return "%";
				}
				case BinaryOperator.ExclusiveOr:
				{
					return "^";
				}
				case BinaryOperator.LeftShift:
				{
					return "<<";
				}
				case BinaryOperator.RightShift:
				{
					return ">>";
				}
				case BinaryOperator.BitwiseAnd:
				{
					return "&";
				}
				case BinaryOperator.BitwiseOr:
				case BinaryOperator.BitwiseExclusiveOr:
				{
					return "|";
				}
				case BinaryOperator.BitwiseNot:
				{
					return "~";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a unary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitUnaryOperation(UnaryOperation operation)
		{
			this.CommandText.Append(GetOperatorName(operation.Operator));
			// TODO: If isbinary: this.Builder.Append(" ");
			this.VisitField(operation.Expression);
		}

		private string GetOperatorName(UnaryOperator op)
		{
			switch (op)
			{
				case UnaryOperator.Not:
				{
					// TODO: return IsBoolean(unary.Expression.Type) ? "NOT" : "~";
					return "NOT ";
				}
				case UnaryOperator.Negate:
				{
					return "-";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a literal part.
		/// </summary>
		/// <param name="literalPart">The literal part.</param>
		protected virtual void VisitLiteralPart(LiteralPart literalPart)
		{
			this.CommandText.Append(literalPart.Value);
		}

		/// <summary>
		/// Visits a select expression.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectExpression(SelectExpression select)
		{
			this.CommandText.Append("(");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(select.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			if (!string.IsNullOrEmpty(select.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(select.Alias);
				this.CommandText.Append("]");
			}
			this.Indent(Indentation.Outer);
		}

		private void AppendNewLine(Indentation style)
		{
			this.CommandText.AppendLine();
			this.Indent(style);
			for (var i = 0; i < this.Depth * IndentationWidth; i++)
			{
				this.CommandText.Append(" ");
			}
		}

		private void Indent(Indentation style)
		{
			if (style == Indentation.Inner)
			{
				this.Depth += 1;
			}
			else if (style == Indentation.Outer)
			{
				this.Depth -= 1;
				System.Diagnostics.Debug.Assert(this.Depth >= 0);
			}
		}
	}

	/// <summary>
	/// Converts QueryModels into Select statements for passing to the database.
	/// </summary>
	public class StatementCreator : QueryModelVisitorBase
	{
		private DatabaseMapper Configuration { get; set; }

        private bool AliasTables { get; set; }

		private SelectStatement SelectStatement { get; set; }

		private StatementCreator(DatabaseMapper mapper, bool aliasTables)
		{
			this.Configuration = mapper;
            this.AliasTables = aliasTables;
			this.SelectStatement = new SelectStatement();
		}

		/// <summary>
		/// Visits the specified query model.
		/// </summary>
		/// <param name="queryModel">The query model.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static SelectStatement Visit(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementCreator(mapper, aliasTables);
			queryModel.Accept(visitor);
			return visitor.SelectStatement;
		}

		/// <summary>
		/// Visits the statement conditions.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="conditions">The conditions.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static ConditionCollection VisitStatementConditions<T>(Expression<Func<T, bool>> conditions, DatabaseMapper mapper, bool aliasTables)
		{
			// Build a new query
			var queryParser = QueryParser.CreateDefault();
			var queryExecutor = new StatementExecutor();
			var query = new StatementQuery<T>(queryParser, queryExecutor);

			// Create an expression to select from the query with the conditions so that we have a sequence for Re-Linq to parse
			var expression = Expression.Call(
				typeof(Queryable),
				"Where",
				new Type[] { query.ElementType },
				query.Expression,
				conditions);

			// Parse the expression with Re-Linq
			var queryModel = queryParser.GetParsedQuery(expression);

			// Get the conditions from the query model
			var visitor = new StatementCreator(mapper, aliasTables);
			visitor.SelectStatement = new SelectStatement();
			queryModel.Accept(visitor);
			return visitor.SelectStatement.Conditions;
		}

		/// <summary>
		/// Visits the select clause.
		/// </summary>
		/// <param name="selectClause">The select clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
		{
			if (selectClause.Selector.NodeType != ExpressionType.Extension)
			{
				var fields = StatementPartCreator.Visit(queryModel, selectClause.Selector, this.Configuration, this.AliasTables);
				this.SelectStatement.SourceFields.Add((SourceExpression)fields);
			}

			base.VisitSelectClause(selectClause, queryModel);
		}

		/// <summary>
		/// Visits the main from clause.
		/// </summary>
		/// <param name="fromClause">From clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
		{
			if (this.Configuration.IsFunction(fromClause.ItemType))
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var functionName = this.Configuration.GetFunctionName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new UserDefinedFunction(functionName) { Alias = alias, Schema = schemaName };
			}
			else
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var tableName = this.Configuration.GetTableName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new Table(tableName) { Alias = alias, Schema = schemaName };
			}
			base.VisitMainFromClause(fromClause, queryModel);
		}

		/// <summary>
		/// Visits the join clause.
		/// </summary>
		/// <param name="joinClause">The join clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
		{
			// TODO: This seems heavy...
			// TODO: And like it's only going to deal with certain types of joins
			var table = (Table)StatementPartCreator.Visit(queryModel, joinClause.InnerSequence, this.Configuration, this.AliasTables);
            table.Alias = joinClause.ItemName.Replace("<generated>", "g");
			var leftColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.OuterKeySelector, this.Configuration, this.AliasTables);
			var rightColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.InnerKeySelector, this.Configuration, this.AliasTables);

			if (leftColumn is FieldCollection leftColumnCollection &&
				rightColumn is FieldCollection rightColumnCollection)
			{
				var joinConditions = new ConditionCollection();
				for (var i = 0; i < leftColumnCollection.Count; i++)
				{
					joinConditions.Add(new Condition(leftColumnCollection[i], SqlOperator.Equals, rightColumnCollection[i]));
				}
				this.SelectStatement.SourceJoins.Add(new Join(table, joinConditions) { JoinType = JoinType.Left });
			}
			else
			{
				this.SelectStatement.SourceJoins.Add(new Join(table, leftColumn, rightColumn) { JoinType = JoinType.Left });
			}

			base.VisitJoinClause(joinClause, queryModel, index);
		}

		/// <summary>
		/// Visits the ordering.
		/// </summary>
		/// <param name="ordering">The ordering.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="orderByClause">The order by clause.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">Invalid ordering direction: {ordering.OrderingDirection}</exception>
		public override void VisitOrdering(Ordering ordering, QueryModel queryModel, OrderByClause orderByClause, int index)
		{
			var column = (Column)StatementPartCreator.Visit(queryModel, ordering.Expression, this.Configuration, this.AliasTables);

			switch (ordering.OrderingDirection)
			{
				case OrderingDirection.Asc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Ascending));
					break;
				}
				case OrderingDirection.Desc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Descending));
					break;
				}
				default:
				{
					throw new InvalidOperationException($"Invalid ordering direction: {ordering.OrderingDirection}");
				}
			}

			base.VisitOrdering(ordering, queryModel, orderByClause, index);
		}

		/// <summary>
		/// Visits the result operator.
		/// </summary>
		/// <param name="resultOperator">The result operator.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">
		/// can't count multiple fields
		/// or
		/// can't sum multiple or no fields
		/// or
		/// can't min multiple or no fields
		/// or
		/// can't max multiple or no fields
		/// or
		/// can't average multiple or no fields
		/// </exception>
		/// <exception cref="NotSupportedException">
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// or
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// </exception>
		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
		{
			if (resultOperator is AnyResultOperator)
			{
				this.SelectStatement.IsAny = true;
				this.SelectStatement.IsAggregate = true;
				return;
			}

			if (resultOperator is AllResultOperator allResults)
			{
				this.SelectStatement.IsAll = true;
				this.SelectStatement.IsAggregate = true;
				var predicate = allResults.Predicate;
				if (predicate != null)
				{
					VisitPredicate(predicate, queryModel);
				}
				return;
			}

			if (resultOperator is ContainsResultOperator containsResult)
			{
				this.SelectStatement.IsContains = true;
				this.SelectStatement.IsAggregate = true;
				var item = containsResult.Item;
				if (item != null && item.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.ContainsItem = new ConstantPart(((ConstantExpression)item).Value);
				}
				return;
			}

			if (resultOperator is FirstResultOperator)
			{
				this.SelectStatement.Limit = 1;
				return;
			}

			if (resultOperator is LastResultOperator)
			{
				this.SelectStatement.Limit = 1;
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			if (resultOperator is CountResultOperator || resultOperator is LongCountResultOperator)
			{
				// Throw an exception if there is more than one field
				if (this.SelectStatement.SourceFields.Count > 1)
				{
					throw new InvalidOperationException("can't count multiple fields");
				}

				// Count the first field
				if (this.SelectStatement.SourceFields.Count == 0)
				{
					this.SelectStatement.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
				}
				else
				{
					this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Count, (Field)this.SelectStatement.SourceFields[0]);
				}

				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is SumResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't sum multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Sum, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MinResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't min multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Min, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MaxResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't max multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Max, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is AverageResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't average multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Average, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is DistinctResultOperator)
			{
				this.SelectStatement.IsDistinct = true;
				return;
			}

			if (resultOperator is TakeResultOperator takeResult)
			{
				var count = takeResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.Limit = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is SkipResultOperator skipResult)
			{
				var count = skipResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.StartIndex = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is ReverseResultOperator)
			{
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			base.VisitResultOperator(resultOperator, queryModel, index);
		}

		/// <summary>
		/// Visits the where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
		{
			VisitPredicate(whereClause.Predicate, queryModel);

			base.VisitWhereClause(whereClause, queryModel, index);
		}

		private void VisitPredicate(Expression predicate, QueryModel queryModel)
		{
			var whereStatement = StatementPartCreator.Visit(queryModel, predicate, this.Configuration, this.AliasTables);
			ConditionExpression condition;
			if (whereStatement is ConditionExpression conditionWhere)
			{
				condition = conditionWhere;
			}
			else if (whereStatement is UnaryOperation unaryWhere && unaryWhere.Expression is ConditionExpression unaryWhereExpression)
			{
				condition = unaryWhereExpression;
			}
			else if (whereStatement is UnaryOperation unaryWhere2 && unaryWhere2.Expression is Column)
			{
				var unary = unaryWhere2;
				var column = (Column)unary.Expression;
				condition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
			}
			else if (whereStatement is ConstantPart constantWhere && constantWhere.Value is bool booleanWhere)
			{
				condition = new Condition() {
					Field = new ConstantPart(booleanWhere),
					Operator = SqlOperator.Equals,
					Value = new ConstantPart(true)
				};
			}
			else if (whereStatement is Column columnWhere && columnWhere.PropertyType == typeof(bool))
			{
				condition = new Condition(columnWhere, SqlOperator.Equals, new ConstantPart(true));
			}
			else
			{
				throw new InvalidOperationException();
			}
			this.SelectStatement.Conditions.Add(condition);
		}
	}

	/// <summary>
	/// A dummy implementation of IQueryExecutor for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <seealso cref="IQueryExecutor" />
	internal class StatementExecutor : IQueryExecutor
	{
		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a collection query, i.e. as a query returning objects of type <typeparamref name="T" />.
		/// The query does not end with a scalar result operator, but it can end with a single result operator, for example
		/// <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" /> or <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" />. In such a case, the returned enumerable must yield exactly
		/// one object (or none if the last result operator allows empty result sets).
		/// </summary>
		/// <typeparam name="T">The type of the items returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a scalar query, i.e. as a query returning a scalar value of type <typeparamref name="T" />.
		/// The query ends with a scalar result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.CountResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SumResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the scalar value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteScalar<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a single object query, i.e. as a query returning a single object of type
		/// <typeparamref name="T" />.
		/// The query ends with a single result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the single value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <param name="returnDefaultWhenEmpty">If <see langword="true" />, the executor must return a default value when its result set is empty;
		/// if <see langword="false" />, it should throw an <see cref="T:System.InvalidOperationException" /> when its result set is empty.</param>
		/// <returns>
		/// A single value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
		{
			throw new NotImplementedException();
		}
	}

	/// <summary>
	/// Converts Expressions (such as those in Re-Linq's QueryModels) into StatementParts.
	/// </summary>
	internal class StatementPartCreator : RelinqExpressionVisitor
	{
		private QueryModel QueryModel { get; set; }

		private DatabaseMapper Configuration { get; set; }

		private bool AliasTables { get; set; }

		private Stack<StatementPart> Stack { get; set; }

		private StatementPartCreator(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			this.QueryModel = queryModel;
			this.Configuration = mapper;
			this.AliasTables = aliasTables;
			this.Stack = new Stack<StatementPart>();
		}

		public static StatementPart Visit(QueryModel queryModel, Expression expression, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementPartCreator(queryModel, mapper, aliasTables);
			visitor.Visit(expression);
			return visitor.Stack.Pop();
		}

		protected override Expression VisitBinary(BinaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				case ExpressionType.ExclusiveOr:
				{
					if (expression.Type == typeof(bool))
					{
						return VisitBinaryConditionCollection(expression);
					}
					else
					{
						return VisitBinaryOperation(expression);
					}
				}
				case ExpressionType.Equal:
				case ExpressionType.NotEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				{
					return VisitBinaryCondition(expression);
				}
				case ExpressionType.Add:
				case ExpressionType.Subtract:
				case ExpressionType.Multiply:
				case ExpressionType.Divide:
				case ExpressionType.Modulo:
				case ExpressionType.LeftShift:
				case ExpressionType.RightShift:
				{
					return VisitBinaryOperation(expression);
				}
			}

			return base.VisitBinary(expression);
		}

		private Expression VisitBinaryConditionCollection(BinaryExpression expression)
		{
			Visit(expression.Left);
			Visit(expression.Right);

			// Convert the conditions on the stack to a collection and set each condition's relationship
			var newCondition = new ConditionCollection();
			for (var i = 0; i < 2; i++)
			{
				ConditionExpression subCondition;
				if (this.Stack.Peek() is ConditionExpression)
				{
					subCondition = (ConditionExpression)this.Stack.Pop();
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp && unaryOp.Expression is ConditionExpression)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					subCondition = (ConditionExpression)unary.Expression;
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp2 && unaryOp2.Expression is Column)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					var column = (Column)unary.Expression;
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
				}
				else if (this.Stack.Peek() is ConstantPart constantPart && constantPart.Value is bool)
				{
					var constant = (ConstantPart)this.Stack.Pop();
					var value = (bool)constant.Value;
					subCondition = new Condition() {
						Field = new ConstantPart(value),
						Operator = SqlOperator.Equals,
						Value = new ConstantPart(true)
					};
				}
				else if (this.Stack.Peek() is Column columnPart && columnPart.PropertyType == typeof(bool))
				{
					var column = (Column)this.Stack.Pop();
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(true));
				}
				else
				{
					break;
				}

				if (subCondition != null)
				{
					newCondition.Insert(0, subCondition);

					if (expression.NodeType == ExpressionType.And ||
						expression.NodeType == ExpressionType.AndAlso)
					{
						subCondition.Relationship = ConditionRelationship.And;
					}
					else
					{
						subCondition.Relationship = ConditionRelationship.Or;
					}
				}
			}

			if (newCondition.Count > 1)
			{
				this.Stack.Push(newCondition);
			}
			else
			{
				this.Stack.Push(newCondition[0]);
			}

			return expression;
		}

		private Expression VisitBinaryCondition(BinaryExpression expression)
		{
			var newCondition = new Condition();
			Visit(expression.Left);
			newCondition.Field = this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Equal:
				{
					newCondition.Operator = SqlOperator.Equals;
					break;
				}
				case ExpressionType.NotEqual:
				{
					newCondition.Operator = SqlOperator.NotEquals;
					break;
				}
				case ExpressionType.LessThan:
				{
					newCondition.Operator = SqlOperator.IsLessThan;
					break;
				}
				case ExpressionType.LessThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsLessThanOrEqualTo;
					break;
				}
				case ExpressionType.GreaterThan:
				{
					newCondition.Operator = SqlOperator.IsGreaterThan;
					break;
				}
				case ExpressionType.GreaterThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsGreaterThanOrEqualTo;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newCondition.Value = this.Stack.Pop();

			if (newCondition.Field.PartType == StatementPartType.FieldCollection)
			{
				// If anonymous types have been passed in for multi-value checking, we need to split
				// them out manually from the field collection and constant part that Relinq creates
				var fields = (FieldCollection)newCondition.Field;
				var value = ((ConstantPart)newCondition.Value).Value;
				var valueList = value.GetType().GetProperties().Select(x => x.GetValue(value, null)).ToList();
				var newConditionCollection = new ConditionCollection();
				// Swap the operator if it's NotEquals
				var op = newCondition.Operator;
				if (op == SqlOperator.NotEquals)
				{
					op = SqlOperator.Equals;
					newConditionCollection.Not = true;
				}
				for (var i = 0; i < fields.Count; i++)
				{
					newConditionCollection.Add(new Condition(fields[i], op, valueList[i]));
				}
				this.Stack.Push(newConditionCollection);
			}
			else
			{
				this.Stack.Push(newCondition);
			}

			return expression;
		}

		private Expression VisitBinaryOperation(BinaryExpression expression)
		{
			var newBinary = new BinaryOperation();
			Visit(expression.Left);
			newBinary.Left = (SourceExpression)this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Add:
				{
					newBinary.Operator = BinaryOperator.Add;
					break;
				}
				case ExpressionType.Subtract:
				{
					newBinary.Operator = BinaryOperator.Subtract;
					break;
				}
				case ExpressionType.Multiply:
				{
					newBinary.Operator = BinaryOperator.Multiply;
					break;
				}
				case ExpressionType.Divide:
				{
					newBinary.Operator = BinaryOperator.Divide;
					break;
				}
				case ExpressionType.Modulo:
				{
					newBinary.Operator = BinaryOperator.Remainder;
					break;
				}
				case ExpressionType.LeftShift:
				{
					newBinary.Operator = BinaryOperator.LeftShift;
					break;
				}
				case ExpressionType.RightShift:
				{
					newBinary.Operator = BinaryOperator.RightShift;
					break;
				}
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				{
					newBinary.Operator = BinaryOperator.BitwiseAnd;
					break;
				}
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				{
					newBinary.Operator = BinaryOperator.BitwiseOr;
					break;
				}
				case ExpressionType.ExclusiveOr:
				{
					newBinary.Operator = BinaryOperator.ExclusiveOr;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newBinary.Right = (SourceExpression)this.Stack.Pop();
			this.Stack.Push(newBinary);

			return expression;
		}

		protected override Expression VisitConditional(ConditionalExpression node)
		{
			var newConditionalCase = new ConditionalCase();
			Visit(node.Test);
			newConditionalCase.Test = this.Stack.Pop();
			Visit(node.IfTrue);
			newConditionalCase.IfTrue = this.Stack.Pop();
			Visit(node.IfFalse);
			newConditionalCase.IfFalse = this.Stack.Pop();
			this.Stack.Push(newConditionalCase);
			return node;
		}

		protected override Expression VisitConstant(ConstantExpression expression)
		{
			if (expression.Value == null)
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			else if (this.Configuration.ShouldMapType(expression.Type))
			{
				var primaryKeyName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
				var property = expression.Value.GetType().GetProperty(primaryKeyName);
				var value = property.GetValue(expression.Value);
				this.Stack.Push(new ConstantPart(value));
			}
			else if (TypeHelper.IsGenericType(expression.Type, typeof(IQueryable<>)))
			{
				var queryType = expression.Value.GetType().GetGenericArguments()[0];
				var tableName = this.Configuration.GetTableName(queryType);
				this.Stack.Push(new Table(tableName));
			}
			else
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			return expression;
		}

		protected override Expression VisitMember(MemberExpression expression)
		{
			if (expression.Member.DeclaringType == typeof(string))
			{
				switch (expression.Member.Name)
				{
					case "Length":
					{
						var newFunction = new StringLengthFunction();
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.DeclaringType == typeof(DateTime) || expression.Member.DeclaringType == typeof(DateTimeOffset))
			{
				switch (expression.Member.Name)
				{
					case "Date":
					{
						var newFunction = new DatePartFunction(DatePart.Date);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Day":
					{
						var newFunction = new DatePartFunction(DatePart.Day);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Month":
					{
						var newFunction = new DatePartFunction(DatePart.Month);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Year":
					{
						var newFunction = new DatePartFunction(DatePart.Year);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Hour":
					{
						var newFunction = new DatePartFunction(DatePart.Hour);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Minute":
					{
						var newFunction = new DatePartFunction(DatePart.Minute);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Second":
					{
						var newFunction = new DatePartFunction(DatePart.Second);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Millisecond":
					{
						var newFunction = new DatePartFunction(DatePart.Millisecond);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfWeek":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfWeek);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfYear":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfYear);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.MemberType == MemberTypes.Property)
			{
				string tableName;
				if (this.AliasTables)
				{
					if (expression.Expression is UnaryExpression unaryExpression)
					{
						var source = (QuerySourceReferenceExpression)unaryExpression.Operand;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else if (expression.Expression is MemberExpression memberExpression)
					{
						var source = (QuerySourceReferenceExpression)memberExpression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else
					{
						var source = (QuerySourceReferenceExpression)expression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
				}
				else
				{
					// The property may be declared on a base type, so we can't just get DeclaringType
					// Instead, we get the type from the expression that was used to reference it
					var propertyType = expression.Expression.Type;

					// HACK: Replace interfaces with actual tables
					//	There has to be a way of intercepting the QueryModel creation??
					if (propertyType.IsInterface)
					{
						propertyType = this.QueryModel.MainFromClause.ItemType;
					}

					tableName = this.Configuration.GetTableName(propertyType);
				}

				var property = (PropertyInfo)expression.Member;
				var columnName = this.Configuration.GetColumnName(property);
				if (this.Configuration.IsRelatedItem(property))
				{
					// TODO: Should this be done here, or when converting the statement to SQL?
					columnName = this.Configuration.GetForeignKeyColumnName(property);
				}
				var newColumn = new Column(tableName, columnName) { PropertyType = property.PropertyType };
				this.Stack.Push(newColumn);
				return expression;
			}

			throw new NotSupportedException($"The member access '{expression.Member}' is not supported");
		}

		protected override Expression VisitMethodCall(MethodCallExpression expression)
		{
			var handled = false;

			if (expression.Method.DeclaringType == typeof(string))
			{
				handled = VisitStringMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(DateTime))
			{
				handled = VisitDateTimeMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(decimal))
			{
				handled = VisitDecimalMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(Math))
			{
				handled = VisitMathMethodCall(expression);
			}

			if (!handled)
			{
				if (expression.Method.Name == "ToString")
				{
					handled = VisitToStringMethodCall(expression);
				}
				else if (expression.Method.Name == "Equals")
				{
					handled = VisitEqualsMethodCall(expression);
				}
				else if (!expression.Method.IsStatic && expression.Method.Name == "CompareTo" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 1)
				{
					handled = VisitCompareToMethodCall(expression);
				}
				else if (expression.Method.IsStatic && expression.Method.Name == "Compare" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 2)
				{
					handled = VisitCompareMethodCall(expression);
				}
			}

			return handled ? expression : base.VisitMethodCall(expression);
		}

		private bool VisitStringMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "StartsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.StartsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "EndsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.EndsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Contains":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.Contains;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Concat":
				{
					var newFunction = new StringConcatenateFunction();
					IList<Expression> args = expression.Arguments;
					if (args.Count == 1 && args[0].NodeType == ExpressionType.NewArrayInit)
					{
						args = ((NewArrayExpression)args[0]).Expressions;
					}
					for (var i = 0; i < args.Count; i++)
					{
						this.Visit(args[i]);
						newFunction.Arguments.Add(this.Stack.Pop());
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IsNullOrEmpty":
				{
					var newCondition = new ConditionCollection();

					var isNullCondition = new Condition();
					this.Visit(expression.Arguments[0]);
					isNullCondition.Field = this.Stack.Pop();
					isNullCondition.Operator = SqlOperator.Equals;
					isNullCondition.Value = new ConstantPart(null);
					newCondition.Add(isNullCondition);

					var notEqualsCondition = new Condition();
					notEqualsCondition.Relationship = ConditionRelationship.Or;
					this.Visit(expression.Arguments[0]);
					notEqualsCondition.Field = this.Stack.Pop();
					notEqualsCondition.Operator = SqlOperator.Equals;
					notEqualsCondition.Value = new ConstantPart("");
					newCondition.Add(notEqualsCondition);

					this.Stack.Push(newCondition);
					return true;
				}
				case "ToUpper":
				case "ToUpperInvariant":
				{
					var newFunction = new StringToUpperFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "ToLower":
				case "ToLowerInvariant":
				{
					var newFunction = new StringToLowerFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Replace":
				{
					var newFunction = new StringReplaceFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.OldValue = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.NewValue = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Substring":
				{
					var newFunction = new SubstringFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Remove":
				{
					var newFunction = new StringRemoveFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IndexOf":
				{
					var newFunction = new StringIndexFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StringToFind = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.StartIndex = this.Stack.Pop();
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Trim":
				{
					var newFunction = new StringTrimFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDateTimeMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "op_Subtract":
				{
					if (expression.Arguments[1].Type == typeof(DateTime))
					{
						var newFunction = new DateDifferenceFunction();
						this.Visit(expression.Arguments[0]);
						newFunction.Date1 = this.Stack.Pop();
						this.Visit(expression.Arguments[1]);
						newFunction.Date2 = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return true;
					}
					break;
				}
				case "AddDays":
				{
					var newFunction = new DateAddFunction(DatePart.Day);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMonths":
				{
					var newFunction = new DateAddFunction(DatePart.Month);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddYears":
				{
					var newFunction = new DateAddFunction(DatePart.Year);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddHours":
				{
					var newFunction = new DateAddFunction(DatePart.Hour);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMinutes":
				{
					var newFunction = new DateAddFunction(DatePart.Minute);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddSeconds":
				{
					var newFunction = new DateAddFunction(DatePart.Second);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMilliseconds":
				{
					var newFunction = new DateAddFunction(DatePart.Millisecond);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDecimalMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Add":
				case "Subtract":
				case "Multiply":
				case "Divide":
				case "Remainder":
				{
					var newOperation = new BinaryOperation();
					this.Visit(expression.Arguments[0]);
					newOperation.Left = (SourceExpression)this.Stack.Pop();
					newOperation.Operator = (BinaryOperator)Enum.Parse(typeof(BinaryOperator), expression.Method.Name);
					this.Visit(expression.Arguments[1]);
					newOperation.Right = (SourceExpression)this.Stack.Pop();
					this.Stack.Push(newOperation);
					return true;
				}
				case "Negate":
				{
					var newFunction = new NumberNegateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Compare":
				{
					this.Visit(Expression.Condition(
						Expression.Equal(expression.Arguments[0], expression.Arguments[1]),
						Expression.Constant(0),
						Expression.Condition(
							Expression.LessThan(expression.Arguments[0], expression.Arguments[1]),
							Expression.Constant(-1),
							Expression.Constant(1)
							)));
					return true;
				}
			}

			return false;
		}

		private bool VisitMathMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Log":
				{
					var newFunction = new NumberLogFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Log10":
				{
					var newFunction = new NumberLog10Function();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sign":
				{
					var newFunction = new NumberSignFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Exp":
				{
					var newFunction = new NumberExponentialFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sqrt":
				{
					var newFunction = new NumberRootFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					newFunction.Root = new ConstantPart(2);
					this.Stack.Push(newFunction);
					return true;
				}
				case "Pow":
				{
					var newFunction = new NumberPowerFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.Power = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Abs":
				{
					var newFunction = new NumberAbsoluteFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sin":
				case "Cos":
				case "Tan":
				case "Acos":
				case "Asin":
				case "Atan":
				case "Atan2":
				{
					var newFunction = new NumberTrigFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Argument2 = this.Stack.Pop();
					}
					newFunction.Function = (TrigFunction)Enum.Parse(typeof(TrigFunction), expression.Method.Name);
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitToStringMethodCall(MethodCallExpression expression)
		{
			if (expression.Object.Type == typeof(string))
			{
				this.Visit(expression.Object);
			}
			else
			{
				var newFunction = new ConvertFunction();
				this.Visit(expression.Arguments[0]);
				newFunction.Expression = (SourceExpression)this.Stack.Pop();
				this.Stack.Push(newFunction);
			}
			return true;
		}

		private bool VisitEqualsMethodCall(MethodCallExpression expression)
		{
			var condition = new Condition();
			condition.Operator = SqlOperator.Equals;
			if (expression.Method.IsStatic && expression.Method.DeclaringType == typeof(object))
			{
				this.Visit(expression.Arguments[0]);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[1]);
				condition.Value = this.Stack.Pop();
			}
			else if (!expression.Method.IsStatic && expression.Arguments.Count > 0 && expression.Arguments[0].Type == expression.Object.Type)
			{
				// TODO: Get the other arguments, most importantly StringComparison
				this.Visit(expression.Object);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[0]);
				condition.Value = this.Stack.Pop();
			}
			this.Stack.Push(condition);
			return true;
		}

		private bool VisitCompareToMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Object);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[0]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		private bool VisitCompareMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Arguments[0]);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[1]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		protected override Expression VisitNew(NewExpression expression)
		{
			if (expression.Type == typeof(DateTime))
			{
				// It's a date, so put its arguments into a DateNewFunction
				var function = new DateNewFunction();
				if (expression.Arguments.Count == 3)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
				}
				else if (expression.Arguments.Count == 6)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
					this.Visit(expression.Arguments[3]);
					function.Hour = this.Stack.Pop();
					this.Visit(expression.Arguments[4]);
					function.Minute = this.Stack.Pop();
					this.Visit(expression.Arguments[5]);
					function.Second = this.Stack.Pop();
				}
				this.Stack.Push(function);
				return expression;
			}
			else if (expression.Arguments.Count > 0)
			{
				// It's a new anonymous object, so get its properties as columns
				var fields = new FieldCollection();
				foreach (var argument in expression.Arguments)
				{
					this.Visit(argument);
					fields.Add((SourceExpression)this.Stack.Pop());
				}
				this.Stack.Push(fields);
				return expression;
			}

			return base.VisitNew(expression);
		}

		protected override Expression VisitUnary(UnaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.Not:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Not;
					Visit(expression.Operand);

					newOperation.Expression = this.Stack.Pop();
					// Push the condition onto the stack instead
					if (newOperation.Expression is Condition newCondition)
					{
						newCondition.Not = true;
						this.Stack.Push(newCondition);
					}
					else
					{
						this.Stack.Push(newOperation);
					}
					return expression;
				}
				case ExpressionType.Negate:
				case ExpressionType.NegateChecked:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Negate;
					Visit(expression.Operand);
					newOperation.Expression = this.Stack.Pop();
					this.Stack.Push(newOperation);
					return expression;
				}
				case ExpressionType.UnaryPlus:
				{
					Visit(expression.Operand);
					return expression;
				}
				case ExpressionType.Convert:
				{
					// Ignore conversions for now
					Visit(expression.Operand);
					return expression;
				}
				default:
				{
					throw new NotSupportedException($"The unary operator '{expression.NodeType}' is not supported");
				}
			}
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
		{
			var tableName = expression.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
			var columnName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
			var newColumn = new Column(tableName, columnName);
			this.Stack.Push(newColumn);

			return base.VisitQuerySourceReference(expression);
		}

		protected override Expression VisitSubQuery(SubQueryExpression expression)
		{
			if (expression.QueryModel.ResultOperators.Count > 0 &&
				expression.QueryModel.ResultOperators[0] is Remotion.Linq.Clauses.ResultOperators.ContainsResultOperator contains)
			{
				// It's an Array.Contains, so we need to convert the subquery into a condition
				var newCondition = new Condition();
				newCondition.Operator = SqlOperator.IsIn;

				Visit(contains.Item);
				newCondition.Field = this.Stack.Pop();

				if (TypeHelper.IsGenericType(expression.QueryModel.MainFromClause.FromExpression.Type, typeof(IQueryable<>)))
				{
					// Create the sub-select statement
					var subselect = StatementCreator.Visit(expression.QueryModel, this.Configuration, true);
					subselect.IsContains = false;
					if (subselect.SourceFields.Count == 0)
					{
						var subselectField = expression.QueryModel.SelectClause.Selector;
						Visit(subselectField);
						subselect.SourceFields.Add((SourceExpression)this.Stack.Pop());
					}
					newCondition.Value = subselect;
				}
				else
				{
					// Just check in the array that was passed
					Visit(expression.QueryModel.MainFromClause.FromExpression);
					newCondition.Value = this.Stack.Pop();
				}

				this.Stack.Push(newCondition);
			}

			return base.VisitSubQuery(expression);
		}

#if DEBUG

		// NOTE: I got sick of re-adding these everytime I wanted to figure out what was going on, so
		// I'm leaving them here in debug only

		protected override Expression VisitBlock(BlockExpression node)
		{
			BreakpointHook();
			return base.VisitBlock(node);
		}

		protected override CatchBlock VisitCatchBlock(CatchBlock node)
		{
			BreakpointHook();
			return base.VisitCatchBlock(node);
		}

		protected override Expression VisitDebugInfo(DebugInfoExpression node)
		{
			BreakpointHook();
			return base.VisitDebugInfo(node);
		}

		protected override Expression VisitDefault(DefaultExpression node)
		{
			BreakpointHook();
			return base.VisitDefault(node);
		}

		protected override Expression VisitDynamic(DynamicExpression node)
		{
			BreakpointHook();
			return base.VisitDynamic(node);
		}

		protected override ElementInit VisitElementInit(ElementInit node)
		{
			BreakpointHook();
			return base.VisitElementInit(node);
		}

		protected override Expression VisitExtension(Expression node)
		{
			BreakpointHook();
			return base.VisitExtension(node);
		}

		protected override Expression VisitGoto(GotoExpression node)
		{
			BreakpointHook();
			return base.VisitGoto(node);
		}

		protected override Expression VisitIndex(IndexExpression node)
		{
			BreakpointHook();
			return base.VisitIndex(node);
		}

		protected override Expression VisitInvocation(InvocationExpression node)
		{
			BreakpointHook();
			return base.VisitInvocation(node);
		}

		protected override Expression VisitLabel(LabelExpression node)
		{
			BreakpointHook();
			return base.VisitLabel(node);
		}

		protected override LabelTarget VisitLabelTarget(LabelTarget node)
		{
			BreakpointHook();
			return base.VisitLabelTarget(node);
		}

		protected override Expression VisitLambda<T>(Expression<T> node)
		{
			BreakpointHook();
			return base.VisitLambda(node);
		}

		protected override Expression VisitListInit(ListInitExpression node)
		{
			BreakpointHook();
			return base.VisitListInit(node);
		}

		protected override Expression VisitLoop(LoopExpression node)
		{
			BreakpointHook();
			return base.VisitLoop(node);
		}

		protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
		{
			BreakpointHook();
			return base.VisitMemberAssignment(node);
		}

		protected override MemberBinding VisitMemberBinding(MemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberBinding(node);
		}

		protected override Expression VisitMemberInit(MemberInitExpression node)
		{
			BreakpointHook();
			return base.VisitMemberInit(node);
		}

		protected override MemberListBinding VisitMemberListBinding(MemberListBinding node)
		{
			BreakpointHook();
			return base.VisitMemberListBinding(node);
		}

		protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberMemberBinding(node);
		}

		protected override Expression VisitNewArray(NewArrayExpression node)
		{
			BreakpointHook();
			return base.VisitNewArray(node);
		}

		protected override Expression VisitParameter(ParameterExpression node)
		{
			BreakpointHook();
			return base.VisitParameter(node);
		}

		protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
		{
			BreakpointHook();
			return base.VisitRuntimeVariables(node);
		}

		protected override Expression VisitSwitch(SwitchExpression node)
		{
			BreakpointHook();
			return base.VisitSwitch(node);
		}

		protected override SwitchCase VisitSwitchCase(SwitchCase node)
		{
			BreakpointHook();
			return base.VisitSwitchCase(node);
		}

		protected override Expression VisitTry(TryExpression node)
		{
			BreakpointHook();
			return base.VisitTry(node);
		}

		protected override Expression VisitTypeBinary(TypeBinaryExpression node)
		{
			BreakpointHook();
			return base.VisitTypeBinary(node);
		}

		// When creating statement parts, put a breakpoint here if you would like to debug
		protected void BreakpointHook()
		{
		}
#endif
	}

	/// <summary>
	/// A dummy implementation of QueryableBase for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso cref="Remotion.Linq.QueryableBase{T}" />
	internal class StatementQuery<T> : QueryableBase<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="queryParser">The <see cref="T:Remotion.Linq.Parsing.Structure.IQueryParser" /> used to parse queries. Specify an instance of
		/// <see cref="T:Remotion.Linq.Parsing.Structure.QueryParser" /> for default behavior. See also <see cref="M:Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault" />.</param>
		/// <param name="executor">The <see cref="T:Remotion.Linq.IQueryExecutor" /> used to execute the query represented by this <see cref="T:Remotion.Linq.QueryableBase`1" />.</param>
		public StatementQuery(IQueryParser queryParser, IQueryExecutor executor)
			: base(new DefaultQueryProvider(typeof(StatementQuery<>), queryParser, executor))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
		/// <param name="expression">The expression.</param>
		public StatementQuery(IQueryProvider provider, Expression expression)
			: base(provider, expression)
		{
		}
	}

	/// <summary>
	/// Contains helper methods for dealing with types.
	/// </summary>
	public static class TypeHelper
	{
		///// <summary>
		///// Finds any interfaces of type IEnumerable on a type.
		///// </summary>
		///// <param name="sequenceType">The type to search for IEnumerable.</param>
		///// <returns></returns>
		//public static Type FindIEnumerable(Type sequenceType)
		//{
		//	if (sequenceType == null || sequenceType == typeof(string))
		//	{
		//		return null;
		//	}

		//	if (sequenceType.IsArray)
		//	{
		//		return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
		//	}

		//	if (sequenceType.IsGenericType)
		//	{
		//		foreach (Type arg in sequenceType.GetGenericArguments())
		//		{
		//			Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
		//			if (ienum.IsAssignableFrom(sequenceType))
		//			{
		//				return ienum;
		//			}
		//		}
		//	}

		//	Type[] interfaces = sequenceType.GetInterfaces();
		//	if (interfaces != null && interfaces.Length > 0)
		//	{
		//		foreach (Type iface in interfaces)
		//		{
		//			Type ienum = FindIEnumerable(iface);
		//			if (ienum != null)
		//				return ienum;
		//		}
		//	}

		//	if (sequenceType.BaseType != null && sequenceType.BaseType != typeof(object))
		//	{
		//		return FindIEnumerable(sequenceType.BaseType);
		//	}

		//	return null;
		//}

		///// <summary>
		///// Gets the type of element contained in a sequence.
		///// </summary>
		///// <param name="sequenceType">The type of the sequence, which must implement an IEnumerable interface.</param>
		///// <returns></returns>
		//public static Type GetElementType(Type sequenceType)
		//{
		//	Type enumerableType = FindIEnumerable(sequenceType);
		//	if (enumerableType == null)
		//	{
		//		return sequenceType;
		//	}
		//	else
		//	{
		//		return enumerableType.GetGenericArguments()[0];
		//	}
		//}

		/// <summary>
		/// Determines whether the specified type is nullable.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns><c>true</c> if the specified type is nullable; otherwise, <c>false</c>.</returns>
		public static bool IsNullableType(Type type)
		{
			return type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		/// <summary>
		/// Gets a non-nullable version of the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public static Type GetNonNullableType(Type type)
		{
			if (IsNullableType(type))
			{
				return type.GetGenericArguments()[0];
			}
			return type;
		}

		///// <summary>
		///// Determines whether the specified type is boolean.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is boolean; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsBoolean(Type type)
		//{
		//	return Type.GetTypeCode(type) == TypeCode.Boolean;
		//}

		//public static bool IsInteger(Type type)
		//{
		//	Type nnType = GetNonNullableType(type);
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.SByte:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.Byte:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		///// <summary>
		///// Determines whether the specified type is numeric.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is numeric; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsNumeric(Type type)
		//{
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.Byte:
		//		case TypeCode.Decimal:
		//		case TypeCode.Double:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.SByte:
		//		case TypeCode.Single:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		//public static bool IsAnonymous(Type type)
		//{
		//	// From http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
		//	// HACK: The only way to detect anonymous types right now.
		//	return
		//		Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false) &&
		//		type.IsGenericType &&
		//		type.Name.Contains("AnonymousType") &&
		//		(type.Name.StartsWith("<>") || type.Name.StartsWith("VB$")) &&
		//		(type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
		//}

		public static bool IsGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/982487/testing-if-object-is-of-generic-type-in-c-sharp
			while (type != null)
			{
				if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
				if (genericType.IsInterface && IsAssignableToGenericType(type, genericType))
				{
					return true;
				}
				type = type.BaseType;
			}
			return false;
		}

		public static bool IsAssignableToGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/5461295/using-isassignablefrom-with-generics
			var interfaceTypes = type.GetInterfaces();

			foreach (var it in interfaceTypes)
			{
				if (it.IsGenericType && it.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
			}

			if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
			{
				return true;
			}

			var baseType = type.BaseType;
			if (baseType == null)
			{
				return false;
			}

			return IsAssignableToGenericType(baseType, genericType);
		}

		///// <summary>
		///// Returns an object of the specified type and whose value is equivalent to the specified object.
		///// </summary>
		///// <param name="value">An object that implements the System.IConvertible interface.</param>
		///// <param name="conversionType">The type of object to return.</param>
		///// <returns>
		///// An object whose type is conversionType and whose value is equivalent to value.-or-A
		///// null reference (Nothing in Visual Basic), if value is null and conversionType
		///// is not a value type.
		///// </returns>
		//public static object ChangeType(object value, Type conversionType)
		//{
		//	if (value == null || value == DBNull.Value)
		//	{
		//		// TODO: Maybe not...
		//		// It would be better to make this generic and pass in the default value
		//		// But that would involve changing emitted code
		//		return null;
		//	}

		//	Type safeType = Nullable.GetUnderlyingType(conversionType) ?? conversionType;
		//	if (safeType.IsEnum)
		//	{
		//		return Enum.ToObject(safeType, value);
		//	}
		//	else
		//	{
		//		return Convert.ChangeType(value, safeType);
		//	}
		//}
	}

	/// <summary>
	/// Contains command text and parameters for running a statement against a database.
	/// </summary>
	public class Command
	{
		/// <summary>
		/// Gets the statement that this command was built from.
		/// </summary>
		/// <value>
		/// The statement.
		/// </value>
		public Statement Statement { get; }

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public string CommandText { get; }

		/// <summary>
		/// Gets the parameters.
		/// </summary>
		/// <value>
		/// The parameters.
		/// </value>
		public IList<object> Parameters { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Command"/> class.
		/// </summary>
		/// <param name="statement">The statement that this command was built from.</param>
		/// <param name="commandText">The command text.</param>
		/// <param name="parameters">The parameters.</param>
		public Command(Statement statement, string commandText, object[] parameters)
		{
			this.Statement = statement;
			this.CommandText = commandText;
			this.Parameters = parameters;
		}
	}

	/// <summary>
	/// Maps .NET objects to database objects.
	/// </summary>
	public class DatabaseMapper
	{
		/// <summary>
		/// Gets the namespace in which entity classes are located.
		/// </summary>
		/// <value>
		/// The entity namespace.
		/// </value>
		public string EntityNamespace { get; set; } = "$";

		/// <summary>
		/// Gets the name of the schema for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetSchemaName(Type type)
		{
			return string.Empty;
		}

		/// <summary>
		/// Gets the name of the table for the supplied type.
		/// </summary>
		/// <remarks>
		/// For a Book item, this would return "Book" by default but might be overridden to return "Books" or something different.
		/// </remarks>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetTableName(Type type)
		{
			return type.Name;
		}

		/// <summary>
		/// Gets the name of the procedure for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetProcedureName(Type type)
		{
			return type.Name.Replace("Procedure", "");
		}

		/// <summary>
		/// Gets the name of the function for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetFunctionName(Type type)
		{
			return type.Name.Replace("Function", "");
		}

		/// <summary>
		/// Gets the name of the column for the supplied property.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetColumnName(PropertyInfo property)
		{
			return property.Name;
		}

		/// <summary>
		/// Gets the name of the primary key column.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetPrimaryKeyColumnName(Type type)
		{
			return "Id";
		}

		/// <summary>
		/// Determines whether the supplied property contains a related entity item.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns>
		///   <c>true</c> if the supplied property contains a related entity item; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsRelatedItem(PropertyInfo property)
		{
			return ShouldMapType(property.PropertyType);
		}

		/// <summary>
		/// Gets the name of the foreign key column for the supplied property.
		/// </summary>
		/// <remarks>
		/// For a Book.Author property, this would return "AuthorID" by default.
		/// </remarks>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetForeignKeyColumnName(PropertyInfo property)
		{
			return property.Name + "Id";
		}

		/// <summary>
		/// Determines whether the supplied type is a stored procedure.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a stored procedure; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsProcedure(Type type)
		{
			return type.Name.EndsWith("Procedure");
		}

		/// <summary>
		/// Determines whether the supplied type is a user-defined function.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a user-defined function; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsFunction(Type type)
		{
			return type.Name.EndsWith("Function");
		}

		/// <summary>
		/// Determines whether the class with the supplied type should be mapped to the database.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual bool ShouldMapType(Type type)
		{
			return (type.Namespace == this.EntityNamespace);
		}
	}

	/// <summary>
	/// Represents a field and aggregate (count, sum, etc) that is used with a select statement.
	/// </summary>
	public class FieldAggregate
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the aggregate (count, sum, etc).
		/// </summary>
		/// <value>
		/// The aggregate.
		/// </value>
		public AggregateType Aggregate { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldAggregate"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="aggregate">The aggregate (count, sum, etc).</param>
		public FieldAggregate(PropertyInfo field, AggregateType aggregate)
		{
			this.Field = field;
			this.Aggregate = aggregate;
		}
	}

	/// <summary>
	/// Represents a field and direction that is used for ordering a statement.
	/// </summary>
	public class FieldOrder
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the order direction (ascending or descending).
		/// </summary>
		/// <value>
		/// The direction.
		/// </value>
		public OrderDirection Direction { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldOrder"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="direction">The order direction (ascending or descending).</param>
		public FieldOrder(PropertyInfo field, OrderDirection direction = OrderDirection.Ascending)
		{
			this.Field = field;
			this.Direction = direction;
		}
	}

	/// <summary>
	/// Represents a field and the value to set it to.
	/// </summary>
	public class FieldValue
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the value to set the field to.
		/// </summary>
		/// <value>
		/// The value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldValue"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="value">The value to set the field to.</param>
		public FieldValue(PropertyInfo field, object value)
		{
			this.Field = field;
			this.Value = value;
		}
	}

	/// <summary>
	/// An interface for building command text and parameters from a statement.
	/// </summary>
	public interface ICommandBuilder
	{
		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		StringBuilder CommandText { get; }

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		List<object> ParameterValues { get; }

		/// <summary>
		/// Visits the statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		void VisitStatement(Statement statement, DatabaseMapper mapper);
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement with the name of the table that records should be selected from.
		/// </summary>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="alias">The alias.</param>
		/// <param name="schema">The schema.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(string tableName, string alias = null, string schema = null)
		{
			return Select.From(new Table(tableName, alias, schema));
		}

		/// <summary>
		/// Creates a select statement with the table that records should be selected from.
		/// </summary>
		/// <param name="table">The table.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Table table)
		{
			return new SelectStatement() { Source = table };
		}

		/// <summary>
		/// Creates a select statement from a join.
		/// </summary>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Join join)
		{
			return new SelectStatement() { Source = join };
		}

		/// <summary>
		/// Creates a select statement from a statement part.
		/// </summary>
		/// <param name="part">The part.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(StatementPart part)
		{
			return new SelectStatement() { Source = part };
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Join join)
		{
			select.SourceJoins.Add(join);
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(joinType, tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(joinType, table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params string[] columnNames)
		{
			select.SourceFields.AddRange(columnNames.Select(cn => new Column(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params SourceExpression[] columns)
		{
			select.SourceFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableNames">The table names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params string[] tableNames)
		{
			select.SourceFieldsFrom.AddRange(tableNames.Select(tn => new Table(tn)));
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tables">The tables.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params Table[] tables)
		{
			select.SourceFieldsFrom.AddRange(tables);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Count, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Count, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Sum, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Sum, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Min, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Min, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Max, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Max, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		public static SelectStatement Average(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Average, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Average(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Average, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Distinct(this SelectStatement select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="skip">The number of records to skip.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Skip(this SelectStatement select, int skip)
		{
			select.StartIndex = skip;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="take">The number of records to take.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Take(this SelectStatement select, int take)
		{
			select.Limit = take;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.AddRange(conditions);
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.And;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.Or;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(cn => new OrderByExpression(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params Column[] columns)
		{
			select.OrderByFields.AddRange(columns.Select(c => new OrderByExpression(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params OrderByExpression[] columns)
		{
			select.OrderByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order descendingly by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderByDescending(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(c => new OrderByExpression(c, OrderDirection.Descending)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params string[] columnNames)
		{
			select.GroupByFields.AddRange(columnNames.Select(c => new Column(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params Column[] columns)
		{
			select.GroupByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds another statement to the select statement as a UNION.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="union">The union.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Union(this SelectStatement select, SelectStatement union)
		{
			select.UnionStatements.Add(union);
			return select;
		}

		/// <summary>
		/// Sets additional paths to include when loading the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="path">The path.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Include(this SelectStatement select, string path)
		{
			select.IncludePaths.Add(path);
			return select;
		}
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement from a type corresponding to the table that records should be selected from.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="alias">The alias.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> From<T>(string alias = null)
		{
			return new SelectStatement<T>(alias);
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		/// <exception cref="InvalidOperationException"></exception>
		public static SelectStatement<T> Columns<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			var field = FuncToPropertyInfo(property, true);
			if (field == null)
			{
				if (property.Body is NewExpression anonObject)
				{
					// It's a new anonymous object, so add each of its arguments
					foreach (var anonArg in anonObject.Arguments)
					{
						if (anonArg is MemberExpression mex)
						{
							select.SourceFields.Add((PropertyInfo)mex.Member);
						}
					}
				}
				else
				{
					throw new InvalidOperationException();
				}
			}
			else
			{
				select.SourceFields.Add(field);
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Distinct<T>(this SelectStatement<T> select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the select statement to count records matching the supplied condition.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select.And(condition);
		}

		/// <summary>
		/// Sets the select statement to count all records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select;
		}

		/// <summary>
		/// Sets the select statement to sum the supplied properties.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Sum<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.AggregateFields.Add(new FieldAggregate(FuncToPropertyInfo(property), AggregateType.Sum));
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="startIndex">The start index.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Skip<T>(this SelectStatement<T> select, int startIndex)
		{
			select.StartIndex = startIndex;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="limit">The limit.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Take<T>(this SelectStatement<T> select, int limit)
		{
			select.Limit = limit;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Where<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			return select.And(condition);
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> And<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.AndAlso(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Or<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.OrElse(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to group by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> GroupBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.GroupByFields.Add(FuncToPropertyInfo(property));
			return select;
		}

		private static PropertyInfo FuncToPropertyInfo<T>(Expression<Func<T, object>> selector, bool returnNull = false)
		{
			if (selector.Body is MemberExpression mex)
			{
				return (PropertyInfo)mex.Member;
			}
			else if (selector.Body is UnaryExpression uex)
			{
				// Throw away converts
				if (uex.Operand is MemberExpression omex)
				{
					return (PropertyInfo)omex.Member;
				}
			}

			// HACK: Yes, this is ugly!
			if (returnNull)
			{
				return null;
			}
			else
			{
				throw new InvalidOperationException();
			}
		}
	}

	/// <summary>
	/// Builds command text and parameters from a statement for use in an SQL database.
	/// </summary>
	/// <seealso cref="CS.QueryBuilder.ICommandBuilder" />
	public class SqlCommandBuilder : ICommandBuilder
	{
		private const int IndentationWidth = 2;

		private enum Indentation
		{
			Same,
			Inner,
			Outer
		}

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public StringBuilder CommandText { get; } = new StringBuilder();

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		public List<object> ParameterValues { get; } = new List<object>();

		private int Depth { get; set; }

		private bool IsNested { get; set; }

		/// <summary>
		/// Visits a statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		/// <exception cref="NotSupportedException"></exception>
		public void VisitStatement(Statement statement, DatabaseMapper mapper)
		{
			switch (statement.PartType)
			{
				case StatementPartType.Select:
				{
					VisitSelect((SelectStatement)statement);
					break;
				}
				case StatementPartType.GenericSelect:
				{
					var select = (SelectStatement)((GenericStatement)statement).CreateStatement(mapper);
					VisitSelect(select);
					break;
				}

				default:
				{
					// TODO:
					throw new NotSupportedException();
				}
			}
		}

		/// <summary>
		/// Visits a constant part.
		/// </summary>
		/// <param name="constant">The constant part.</param>
		protected virtual void VisitConstant(ConstantPart constant)
		{
			VisitObject(constant.Value);
			if (!string.IsNullOrEmpty(constant.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(constant.Alias);
				this.CommandText.Append("]");
			}
		}

		/// <summary>
		/// Visits an object.
		/// </summary>
		/// <param name="value">The object.</param>
		protected virtual void VisitObject(object value)
		{
			if (value == null)
			{
				this.CommandText.Append("NULL");
			}
			else if (value.GetType() == typeof(bool))
			{
				this.CommandText.Append(((bool)value) ? "1" : "0");
			}
			else if (value.GetType() == typeof(string) && value.ToString().Length == 0)
			{
				this.CommandText.Append("''");
			}
			else if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
			{
				var firstValue = true;
				foreach (var innerValue in enumerable)
				{
					if (!firstValue)
					{
						this.CommandText.Append(", ");
					}
					firstValue = false;
					if (innerValue is ConstantPart constantValue)
					{
						this.VisitConstant(constantValue);
					}
					else
					{
						this.VisitObject(innerValue);
					}
				}
			}
			else
			{
				var index = this.ParameterValues.IndexOf(value);
				if (index != -1)
				{
					this.CommandText.Append("@");
					this.CommandText.Append(index);
				}
				else
				{
					this.CommandText.Append("@");
					this.CommandText.Append(this.ParameterValues.Count);
					if (value.GetType().IsEnum)
					{
						this.ParameterValues.Add(Convert.ToInt64(value));
					}
					else
					{
						this.ParameterValues.Add(value);
					}
				}
			}
		}

		/// <summary>
		/// Visits a select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelect(SelectStatement select)
		{
			// TODO: If we're using SQL Server 2012 we should just use the OFFSET keyword
			if (select.StartIndex > 0)
			{
				VisitSelectWithRowNumber(select);
				return;
			}

			if (select.IsAny)
			{
				VisitSelectWithAny(select);
				return;
			}

			if (select.IsAll)
			{
				VisitSelectWithAll(select);
				return;
			}

			if (select.IsContains)
			{
				VisitSelectWithContains(select);
				return;
			}

			// If any of the fields have aggregates that aren't grouped, remove the ordering as SQL Server doesn't like it
			// TODO: Only if they aren't grouped
			if (select.SourceFields.Any(f => f is Aggregate))
			{
				select.OrderByFields.Clear();
			}

			this.CommandText.Append("SELECT ");
			if (select.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and remove this method
				VisitLimitAtStart(select);
			}
			if (select.SourceFieldsFrom.Count > 0)
			{
				VisitSourceFieldsFrom(select);
			}
			if (select.SourceFields.Count > 0)
			{
				VisitSourceFields(select);
			}
			if (select.SourceFieldsFrom.Count == 0 && select.SourceFields.Count == 0)
			{
				if (this.IsNested)
				{
					// TODO: Rename tmp, it sucks
					this.CommandText.Append("NULL ");
					this.CommandText.Append("AS tmp");
				}
				else
				{
					// TODO: When to use "*" vs "NULL"?
					this.CommandText.Append("*");
				}
			}
			if (select.Source != null)
			{
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append("FROM ");
				this.VisitSource(select.Source);
			}
			if (select.SourceJoins != null)
			{
				for (var i = 0; i < select.SourceJoins.Count; i++)
				{
					this.AppendNewLine(Indentation.Same);
					this.VisitJoin(select.SourceJoins[i]);
				}
			}
			if (select.Conditions.Count > 0)
			{
				VisitConditions(select.Conditions);
			}
			if (select.GroupByFields.Count > 0)
			{
				VisitGroupBy(select);
			}
			if (select.OrderByFields.Count > 0 && !select.IsAggregate)
			{
				VisitOrderBy(select);
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and rename this method
				VisitLimitAtEnd(select);
			}
			foreach (var union in select.UnionStatements)
			{
				this.CommandText.AppendLine();
				this.CommandText.AppendLine("UNION ALL");
				VisitSelect(union);
			}
		}

		/// <summary>
		/// Visits a select statement with a row number.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithRowNumber(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT Fields
			// FROM (SELECT Fields,
			//		ROW_NUMBER() OVER (ORDER BY OrderFields) AS RowNumber
			//		FROM Table
			//		WHERE Condition)
			// WHERE RowNumber > Start AND RowNumber <= End
			// ORDER BY OrderFields

			// Clone the select and add the RowNumber field to it
			var inner = Select.From(select.Source);
			inner.SourceJoins.AddRange(select.SourceJoins);
			inner.Alias = "RowNumberTable";
			inner.SourceFields.AddRange(select.SourceFields);
			inner.SourceFields.Add(new RowNumber(select.OrderByFields.ToArray()));
			inner.Conditions.AddRange(select.Conditions);

			// If the original table selected all fields, we need to add another field to select them ourselves
			if (!select.SourceFields.Any())
			{
				if (select.Source is Table table)
				{
					inner.SourceFields.Add(new Column(table.Name, "*"));
				}
			}

			// Clone the select and change its source
			var outer = Select.From(inner);
			foreach (var field in select.SourceFields)
			{
				if (field is Column column)
				{
					outer.SourceFields.Add(new Column(inner.Alias, column.Name));
				}
			}
			if (select.StartIndex > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsGreaterThan, select.StartIndex));
			}
			if (select.Limit > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsLessThanOrEqualTo, select.StartIndex + select.Limit));
			}
			outer.OrderByFields.Add(new OrderByExpression("RowNumber"));

			// Visit the outer select
			VisitSelect(outer);
		}

		/// <summary>
		/// Visits a select statement with ANY.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAny(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN EXISTS (");
			this.Indent(Indentation.Inner);

			select.IsAny = false;

			this.VisitSelect(select);

			select.IsAny = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with ALL.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAll(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN NOT EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE NOT Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN NOT EXISTS (");
			this.Indent(Indentation.Inner);

			var not = select.Conditions.Not;
			select.IsAll = false;
			select.Conditions.Not = !not;

			this.VisitSelect(select);

			select.IsAll = true;
			select.Conditions.Not = not;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with CONTAINS.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithContains(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN @0 IN (
			//		SELECT Fields
			//		FROM Table
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN ");
			this.VisitField(select.ContainsItem);
			this.CommandText.Append(" IN (");
			this.Indent(Indentation.Inner);

			select.IsContains = false;

			this.VisitSelect(select);

			select.IsContains = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits an update statement.
		/// </summary>
		/// <param name="update">The update statement.</param>
		/// <exception cref="InvalidOperationException">An update statement must have at least one condition to avoid accidentally updating all data in a table</exception>
		//protected virtual void VisitUpdate(UpdateStatement update)
		//{
		//	this.CommandText.Append("UPDATE ");
		//	this.VisitTable(update.Target);
		//	this.CommandText.Append(" SET");
		//	this.AppendNewLine(Indentation.Inner);
		//	if (update.SetValues != null && update.SetValues.Count > 0)
		//	{
		//		for (var i = 0; i < update.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(",");
		//				this.AppendNewLine(Indentation.Same);
		//			}
		//			this.VisitColumn(update.SetValues[i].Column, ignoreTablePrefix: true);
		//			this.CommandText.Append(" = ");
		//			if (update.SetValues[i].Value is SelectStatement select)
		//			{
		//				// Special case - ensure a select statement is surrounded with brackets
		//				this.VisitField(new SelectExpression(select));
		//			}
		//			else
		//			{
		//				this.VisitField(update.SetValues[i].Value);
		//			}
		//		}
		//	}
		//	this.Indent(Indentation.Outer);
		//	if (update.Conditions != null && update.Conditions.Count > 0)
		//	{
		//		VisitConditions(update.Conditions);
		//	}
		//	else
		//	{
		//		throw new InvalidOperationException("An update statement must have at least one condition to avoid accidentally updating all data in a table");
		//	}
		//}

		/// <summary>
		/// Visits an insert statement.
		/// </summary>
		/// <param name="insert">The insert statement.</param>
		//protected virtual void VisitInsert(InsertStatement insert)
		//{
		//	this.CommandText.Append("INSERT INTO ");
		//	this.VisitTable(insert.Target);
		//	if (insert.SetValues != null && insert.SetValues.Count > 0)
		//	{
		//		this.CommandText.Append(" (");
		//		for (var i = 0; i < insert.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitColumn(insert.SetValues[i].Column);
		//		}
		//		this.CommandText.Append(")");
		//		this.AppendNewLine(Indentation.Same);
		//		this.CommandText.Append("VALUES (");
		//		for (var i = 0; i < insert.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitField(insert.SetValues[i].Value);
		//		}
		//		this.CommandText.Append(")");
		//	}
		//	else if (insert.TargetFields != null && insert.TargetFields.Count > 0 && insert.Source != null)
		//	{
		//		this.CommandText.Append(" (");
		//		for (var i = 0; i < insert.TargetFields.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitColumn(insert.TargetFields[i]);
		//		}
		//		this.CommandText.Append(")");
		//		this.AppendNewLine(Indentation.Same);
		//		this.VisitSelect(insert.Source);
		//	}
		//	else
		//	{
		//		this.CommandText.Append(" DEFAULT VALUES");
		//	}
		//}

		/// <summary>
		/// Visits a delete statement.
		/// </summary>
		/// <param name="delete">The delete statement.</param>
		/// <exception cref="InvalidOperationException">A delete statement must have at least one condition to avoid accidentally deleting all data in a table</exception>
		//protected virtual void VisitDelete(DeleteStatement delete)
		//{
		//	this.CommandText.Append("DELETE FROM ");
		//	this.VisitTable(delete.Target);
		//	if (delete.Conditions != null && delete.Conditions.Count > 0)
		//	{
		//		VisitConditions(delete.Conditions);
		//	}
		//	else
		//	{
		//		throw new InvalidOperationException("A delete statement must have at least one condition to avoid accidentally deleting all data in a table");
		//	}
		//}

		/// <summary>
		/// Visits a source fields.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFields(SelectStatement select)
		{
			for (var i = 0; i < select.SourceFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.SourceFields[i]);
			}
		}

		/// <summary>
		/// Visits a source fields from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFieldsFrom(SelectStatement select)
		{
			// TODO: Should the SourceFieldsFrom actually be its own class?
			for (var i = 0; i < select.SourceFieldsFrom.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitTable(select.SourceFieldsFrom[i]);
				this.CommandText.Append(".*");
			}
			if (select.SourceFields.Count > 0)
			{
				this.CommandText.Append(", ");
			}
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="conditions">The condition collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditions(ConditionCollection conditions)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("WHERE ");
			if (conditions.Not)
			{
				this.CommandText.Append("NOT ");
			}
			for (var i = 0; i < conditions.Count; i++)
			{
				if (i > 0)
				{
					this.AppendNewLine(Indentation.Same);
					switch (conditions[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(conditions[i]);
			}
		}

		/// <summary>
		/// Visits a group by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitGroupBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("GROUP BY ");
			for (var i = 0; i < select.GroupByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.GroupByFields[i]);
			}
		}

		/// <summary>
		/// Visits an order by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitOrderBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("ORDER BY ");
			for (var i = 0; i < select.OrderByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.OrderByFields[i].Expression);
				if (select.OrderByFields[i].Direction != OrderDirection.Ascending)
				{
					this.CommandText.Append(" DESC");
				}
			}
		}

		/// <summary>
		/// Visits a limit at start.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtStart(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a limit at end.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtEnd(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a field.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitField(StatementPart field)
		{
			switch (field.PartType)
			{
				case StatementPartType.Column:
				{
					this.VisitColumn((Column)field);
					break;
				}
				case StatementPartType.RowNumber:
				{
					this.VisitRowNumber((RowNumber)field);
					break;
				}
				case StatementPartType.Aggregate:
				{
					this.VisitAggregate((Aggregate)field);
					break;
				}
				case StatementPartType.ConditionalCase:
				{
					this.VisitConditionalCase((ConditionalCase)field);
					break;
				}
				case StatementPartType.ConditionPredicate:
				{
					this.VisitConditionPredicate((ConditionPredicate)field);
					break;
				}
				case StatementPartType.Exists:
				{
					this.VisitExists((Exists)field);
					break;
				}
				case StatementPartType.CoalesceFunction:
				{
					this.VisitCoalesceFunction((CoalesceFunction)field);
					break;
				}
				case StatementPartType.ConvertFunction:
				{
					this.VisitConvertFunction((ConvertFunction)field);
					break;
				}
				case StatementPartType.StringLengthFunction:
				{
					this.VisitStringLengthFunction((StringLengthFunction)field);
					break;
				}
				case StatementPartType.SubstringFunction:
				{
					this.VisitSubstringFunction((SubstringFunction)field);
					break;
				}
				case StatementPartType.StringRemoveFunction:
				{
					this.VisitStringRemoveFunction((StringRemoveFunction)field);
					break;
				}
				case StatementPartType.StringIndexFunction:
				{
					this.VisitStringCharIndexFunction((StringIndexFunction)field);
					break;
				}
				case StatementPartType.StringToUpperFunction:
				{
					this.VisitStringToUpperFunction((StringToUpperFunction)field);
					break;
				}
				case StatementPartType.StringToLowerFunction:
				{
					this.VisitStringToLowerFunction((StringToLowerFunction)field);
					break;
				}
				case StatementPartType.StringReplaceFunction:
				{
					this.VisitStringReplaceFunction((StringReplaceFunction)field);
					break;
				}
				case StatementPartType.StringTrimFunction:
				{
					this.VisitStringTrimFunction((StringTrimFunction)field);
					break;
				}
				case StatementPartType.StringCompareFunction:
				{
					this.VisitStringCompareFunction((StringCompareFunction)field);
					break;
				}
				case StatementPartType.StringConcatenateFunction:
				{
					this.VisitStringConcatenateFunction((StringConcatenateFunction)field);
					break;
				}
				case StatementPartType.DatePartFunction:
				{
					this.VisitDatePartFunction((DatePartFunction)field);
					break;
				}
				case StatementPartType.DateAddFunction:
				{
					this.VisitDateAddFunction((DateAddFunction)field);
					break;
				}
				case StatementPartType.DateNewFunction:
				{
					this.VisitDateNewFunction((DateNewFunction)field);
					break;
				}
				case StatementPartType.DateDifferenceFunction:
				{
					this.VisitDateDifferenceFunction((DateDifferenceFunction)field);
					break;
				}
				case StatementPartType.NumberAbsoluteFunction:
				{
					this.VisitNumberAbsoluteFunction((NumberAbsoluteFunction)field);
					break;
				}
				case StatementPartType.NumberNegateFunction:
				{
					this.VisitNumberNegateFunction((NumberNegateFunction)field);
					break;
				}
				case StatementPartType.NumberCeilingFunction:
				{
					this.VisitNumberCeilingFunction((NumberCeilingFunction)field);
					break;
				}
				case StatementPartType.NumberFloorFunction:
				{
					this.VisitNumberFloorFunction((NumberFloorFunction)field);
					break;
				}
				case StatementPartType.NumberRoundFunction:
				{
					this.VisitNumberRoundFunction((NumberRoundFunction)field);
					break;
				}
				case StatementPartType.NumberTruncateFunction:
				{
					this.VisitNumberTruncateFunction((NumberTruncateFunction)field);
					break;
				}
				case StatementPartType.NumberSignFunction:
				{
					this.VisitNumberSignFunction((NumberSignFunction)field);
					break;
				}
				case StatementPartType.NumberPowerFunction:
				{
					this.VisitNumberPowerFunction((NumberPowerFunction)field);
					break;
				}
				case StatementPartType.NumberRootFunction:
				{
					this.VisitNumberRootFunction((NumberRootFunction)field);
					break;
				}
				case StatementPartType.NumberExponentialFunction:
				{
					this.VisitNumberExponentialFunction((NumberExponentialFunction)field);
					break;
				}
				case StatementPartType.NumberLogFunction:
				{
					this.VisitNumberLogFunction((NumberLogFunction)field);
					break;
				}
				case StatementPartType.NumberLog10Function:
				{
					this.VisitNumberLog10Function((NumberLog10Function)field);
					break;
				}
				case StatementPartType.NumberTrigFunction:
				{
					this.VisitNumberTrigFunction((NumberTrigFunction)field);
					break;
				}
				case StatementPartType.BinaryOperation:
				{
					this.VisitBinaryOperation((BinaryOperation)field);
					break;
				}
				case StatementPartType.UnaryOperation:
				{
					this.VisitUnaryOperation((UnaryOperation)field);
					break;
				}
				case StatementPartType.LiteralPart:
				{
					this.VisitLiteralPart((LiteralPart)field);
					break;
				}
				case StatementPartType.Select:
				{
					this.VisitSelect((SelectStatement)field);
					break;
				}
				case StatementPartType.ConstantPart:
				{
					this.VisitConstant((ConstantPart)field);
					break;
				}
				case StatementPartType.Condition:
				{
					this.VisitCondition((Condition)field);
					break;
				}
				case StatementPartType.FieldCollection:
				{
					var collection = (FieldCollection)field;
					for (var i = 0; i < collection.Count; i++)
					{
						if (i > 0)
						{
							this.CommandText.Append(", ");
						}
						this.VisitField(collection[i]);
					}
					break;
				}
				case StatementPartType.SelectExpression:
				{
					this.VisitSelectExpression((SelectExpression)field);
					break;
				}
				default:
				{
					// TODO: Words for all exceptions
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a column.
		/// </summary>
		/// <param name="column">The column.</param>
		/// <param name="ignoreTablePrefix">if set to <c>true</c> [ignore table prefix].</param>
		protected virtual void VisitColumn(Column column, bool ignoreTablePrefix = false)
		{
			if (!ignoreTablePrefix && column.Table != null && !string.IsNullOrEmpty(column.Table.Name))
			{
				if (!string.IsNullOrEmpty(column.Table.Alias))
				{
					this.CommandText.Append("[");
					this.CommandText.Append(column.Table.Alias);
					this.CommandText.Append("]");
				}
				else
				{
					VisitTable(column.Table);
				}
				this.CommandText.Append(".");
			}

			if (column.Name == "*")
			{
				this.CommandText.Append("*");
			}
			else if (column.Name.StartsWith("@"))
			{
				// HACK: Allowing the user to pass parameter names in with new Column("@ParameterID")
				// but it might be better to require new Parameter("@ParameterID")
				this.CommandText.Append(column.Name);
			}
			else
			{
				this.CommandText.Append("[");
				this.CommandText.Append(column.Name);
				this.CommandText.Append("]");
				if (!string.IsNullOrEmpty(column.Alias))
				{
					this.CommandText.Append(" AS [");
					this.CommandText.Append(column.Alias);
					this.CommandText.Append("]");
				}
			}
		}

		/// <summary>
		/// Visits a source.
		/// </summary>
		/// <param name="source">The source.</param>
		/// <exception cref="InvalidOperationException">Select source is not valid type</exception>
		protected virtual void VisitSource(StatementPart source)
		{
			var previousIsNested = this.IsNested;
			this.IsNested = true;
			switch (source.PartType)
			{
				case StatementPartType.Table:
				{
					var table = (Table)source;
					this.VisitTable(table);
					if (!string.IsNullOrEmpty(table.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(table.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				case StatementPartType.Select:
				{
					var select = (SelectStatement)source;
					this.CommandText.Append("(");
					this.AppendNewLine(Indentation.Inner);
					this.VisitSelect(select);
					this.AppendNewLine(Indentation.Same);
					this.CommandText.Append(")");
					if (!string.IsNullOrEmpty(select.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(select.Alias);
						this.CommandText.Append("]");
					}
					this.Indent(Indentation.Outer);
					break;
				}
				case StatementPartType.Join:
				{
					this.VisitJoin((Join)source);
					break;
				}
				case StatementPartType.UserDefinedFunction:
				{
					var function = (UserDefinedFunction)source;
					this.VisitUserDefinedFunction(function);
					if (!string.IsNullOrEmpty(function.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(function.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				default:
				{
					throw new InvalidOperationException("Select source is not valid type");
				}
			}
			this.IsNested = previousIsNested;
		}

		/// <summary>
		/// Visits a table.
		/// </summary>
		/// <param name="table">The table.</param>
		protected virtual void VisitTable(Table table)
		{
			if (!string.IsNullOrEmpty(table.Schema))
			{
				this.CommandText.Append("[");
				this.CommandText.Append(table.Schema);
				this.CommandText.Append("]");
				this.CommandText.Append(".");
			}
			this.CommandText.Append("[");
			this.CommandText.Append(table.Name);
			this.CommandText.Append("]");
		}

		/// <summary>
		/// Visits an user defined function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitUserDefinedFunction(UserDefinedFunction function)
		{
			if (!string.IsNullOrEmpty(function.Schema))
			{
				this.CommandText.Append(function.Schema);
				this.CommandText.Append(".");
			}
			this.CommandText.Append(function.Name);
			this.CommandText.Append("(");
			for (var i = 0; i < function.Parameters.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitObject(function.Parameters[i].Value);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a join.
		/// </summary>
		/// <param name="join">The join.</param>
		protected virtual void VisitJoin(Join join)
		{
			switch (join.JoinType)
			{
				case JoinType.Inner:
				{
					this.CommandText.Append("INNER JOIN ");
					break;
				}
				case JoinType.Left:
				{
					this.CommandText.Append("LEFT OUTER JOIN ");
					break;
				}
				case JoinType.Right:
				{
					this.CommandText.Append("RIGHT OUTER JOIN ");
					break;
				}
				case JoinType.Cross:
				{
					this.CommandText.Append("CROSS JOIN ");
					break;
				}
				case JoinType.CrossApply:
				{
					this.CommandText.Append("CROSS APPLY ");
					break;
				}
			}
			this.VisitSource(join.Table);
			if (join.Conditions.Count > 0)
			{
				this.CommandText.Append(" ON ");
				this.VisitConditionCollection(join.Conditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitCondition(ConditionExpression condition)
		{
			// TODO: Should all types of conditions be a class?  Not exposed to the user, because that
			// interface would be gross
			if (condition is Exists existsCondition)
			{
				VisitExists(existsCondition);
				return;
			}

			if (condition.Not)
			{
				this.CommandText.Append("NOT ");
			}

			if (condition is Condition singleCondition)
			{
				VisitCondition(singleCondition);
			}
			else if (condition is ConditionCollection multipleConditions)
			{
				VisitConditionCollection(multipleConditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		/// <exception cref="InvalidOperationException">Invalid operator: " + condition.Operator</exception>
		protected virtual void VisitCondition(Condition condition)
		{
			// Check for null comparisons first
			var fieldIsNull = (condition.Field is ConstantPart constantField && constantField.Value == null);
			var valueIsNull = (condition.Value is ConstantPart constantValue && constantValue.Value == null);
			if ((condition.Operator == SqlOperator.Equals || condition.Operator == SqlOperator.NotEquals) &&
				(fieldIsNull || valueIsNull))
			{
				if (fieldIsNull)
				{
					this.VisitField(condition.Value);
				}
				else if (valueIsNull)
				{
					this.VisitField(condition.Field);
				}
				if (condition.Operator == SqlOperator.Equals)
				{
					this.CommandText.Append(" IS NULL");
				}
				else if (condition.Operator == SqlOperator.NotEquals)
				{
					this.CommandText.Append(" IS NOT NULL");
				}
			}
			else
			{
				switch (condition.Operator)
				{
					case SqlOperator.Equals:
					{
						VisitEqualsCondition(condition);
						break;
					}
					case SqlOperator.NotEquals:
					{
						VisitNotEqualsCondition(condition);
						break;
					}
					case SqlOperator.IsLessThan:
					{
						VisitIsLessThanCondition(condition);
						break;
					}
					case SqlOperator.IsLessThanOrEqualTo:
					{
						VisitIsLessThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThan:
					{
						VisitIsGreaterThanCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThanOrEqualTo:
					{
						VisitIsGreaterThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsIn:
					{
						VisitIsInCondition(condition);
						break;
					}
					case SqlOperator.Contains:
					{
						VisitContainsCondition(condition);
						break;
					}
					case SqlOperator.StartsWith:
					{
						VisitStartsWithCondition(condition);
						break;
					}
					case SqlOperator.EndsWith:
					{
						VisitEndsWithCondition(condition);
						break;
					}
					default:
					{
						throw new InvalidOperationException("Invalid operator: " + condition.Operator);
					}
				}
			}
		}

		/// <summary>
		/// Visits an equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" = ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a not equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitNotEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <> ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" < ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" > ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" >= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is in condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsInCondition(Condition condition)
		{
			// If it's in an empty list, just check against false
			var handled = false;
			if (condition.Value.PartType == StatementPartType.ConstantPart)
			{
				var value = ((ConstantPart)condition.Value).Value;
				if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
				{
					// HACK: Ugh
					var hasThings = false;
					foreach (var thing in enumerable)
					{
						hasThings = true;
						break;
					}
					if (!hasThings)
					{
						handled = true;
						this.CommandText.Append(" 0 <> 0");
					}
				}
			}
			if (!handled)
			{
				this.VisitField(condition.Field);
				this.CommandText.Append(" IN (");
				this.AppendNewLine(Indentation.Inner);
				this.VisitField(condition.Value);
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append(")");
				this.AppendNewLine(Indentation.Outer);
			}
		}

		/// <summary>
		/// Visits a contains condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitContainsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits a starts with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitStartsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits an ends with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEndsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="collection">The collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditionCollection(ConditionCollection collection)
		{
			this.CommandText.Append("(");
			for (var i = 0; i < collection.Count; i++)
			{
				if (i > 0)
				{
					// TODO: make this a visitrelationship method
					this.AppendNewLine(Indentation.Same);
					switch (collection[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(collection[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a conditional case.
		/// </summary>
		/// <param name="conditional">The conditional.</param>
		protected virtual void VisitConditionalCase(ConditionalCase conditional)
		{
			if (conditional.Test is Condition)
			{
				this.CommandText.Append("(CASE WHEN ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" THEN ");
				this.VisitField(conditional.IfTrue);
				var ifFalse = conditional.IfFalse;
				while (ifFalse != null && ifFalse.PartType == StatementPartType.ConditionalCase)
				{
					var subconditional = (ConditionalCase)conditional.IfFalse;
					this.CommandText.Append(" WHEN ");
					this.VisitField(subconditional.Test);
					this.CommandText.Append(" THEN ");
					this.VisitField(subconditional.IfTrue);
					ifFalse = subconditional.IfFalse;
				}
				if (ifFalse != null)
				{
					this.CommandText.Append(" ELSE ");
					this.VisitField(ifFalse);
				}
				this.CommandText.Append(" END)");
			}
			else
			{
				this.CommandText.Append("(CASE ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" WHEN 0 THEN ");
				this.VisitField(conditional.IfFalse);
				this.CommandText.Append(" ELSE ");
				this.VisitField(conditional.IfTrue);
				this.CommandText.Append(" END)");
			}
		}

		/// <summary>
		/// Visits a row number.
		/// </summary>
		/// <param name="rowNumber">The row number.</param>
		protected virtual void VisitRowNumber(RowNumber rowNumber)
		{
			this.CommandText.Append("ROW_NUMBER() OVER(");
			if (rowNumber.OrderByFields != null && rowNumber.OrderByFields.Count > 0)
			{
				this.CommandText.Append("ORDER BY ");
				for (var i = 0; i < rowNumber.OrderByFields.Count; i++)
				{
					if (i > 0)
					{
						this.CommandText.Append(", ");
					}
					this.VisitField(rowNumber.OrderByFields[i].Expression);
					if (rowNumber.OrderByFields[i].Direction != OrderDirection.Ascending)
					{
						this.CommandText.Append(" DESC");
					}
				}
			}
			this.CommandText.Append(") AS RowNumber");
		}

		/// <summary>
		/// Visits an aggregate.
		/// </summary>
		/// <param name="aggregate">The aggregate.</param>
		protected virtual void VisitAggregate(Aggregate aggregate)
		{
			this.CommandText.Append(GetAggregateName(aggregate.AggregateType));
			this.CommandText.Append("(");
			if (aggregate.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (aggregate.Field != null)
			{
				this.VisitField(aggregate.Field);
			}
			else if (aggregate.AggregateType == AggregateType.Count ||
				aggregate.AggregateType == AggregateType.BigCount)
			{
				this.CommandText.Append("*");
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Gets the name of the aggregate.
		/// </summary>
		/// <param name="aggregateType">Type of the aggregate.</param>
		/// <returns></returns>
		/// <exception cref="Exception">Unknown aggregate type: {aggregateType}</exception>
		private string GetAggregateName(AggregateType aggregateType)
		{
			switch (aggregateType)
			{
				case AggregateType.Count:
				{
					return "COUNT";
				}
				case AggregateType.BigCount:
				{
					return "COUNT_BIG";
				}
				case AggregateType.Min:
				{
					return "MIN";
				}
				case AggregateType.Max:
				{
					return "MAX";
				}
				case AggregateType.Sum:
				{
					return "SUM";
				}
				case AggregateType.Average:
				{
					return "AVG";
				}
				default:
				{
					throw new Exception($"Unknown aggregate type: {aggregateType}");
				}
			}
		}

		/// <summary>
		/// Visits a condition predicate.
		/// </summary>
		/// <param name="predicate">The predicate.</param>
		protected virtual void VisitConditionPredicate(ConditionPredicate predicate)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(predicate.Predicate);
			this.CommandText.Append(" THEN 1 ELSE 0 END)");
		}

		/// <summary>
		/// Visits an exists.
		/// </summary>
		/// <param name="exists">The exists.</param>
		protected virtual void VisitExists(Exists exists)
		{
			if (exists.Not)
			{
				this.CommandText.Append("NOT ");
			}
			this.CommandText.Append("EXISTS (");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(exists.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			this.Indent(Indentation.Outer);
		}

		/// <summary>
		/// Visits a coalesce function.
		/// </summary>
		/// <param name="coalesce">The coalesce.</param>
		protected virtual void VisitCoalesceFunction(CoalesceFunction coalesce)
		{
			StatementPart first = coalesce.Arguments[0];
			StatementPart second = coalesce.Arguments[1];

			this.CommandText.Append("COALESCE(");
			this.VisitField(first);
			this.CommandText.Append(", ");
			while (second.PartType == StatementPartType.CoalesceFunction)
			{
				var secondCoalesce = (CoalesceFunction)second;
				this.VisitField(secondCoalesce.Arguments[0]);
				this.CommandText.Append(", ");
				second = secondCoalesce.Arguments[1];
			}
			this.VisitField(second);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a function.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="arguments">The arguments.</param>
		protected virtual void VisitFunction(string name, params StatementPart[] arguments)
		{
			this.CommandText.Append(name);
			this.CommandText.Append("(");
			for (var i = 0; i < arguments.Length; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(arguments[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a convert function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitConvertFunction(ConvertFunction function)
		{
			// TODO: Handle more types
			this.CommandText.Append("CONVERT(VARCHAR, ");
			this.VisitField(function.Expression);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string length function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringLengthFunction(StringLengthFunction function)
		{
			VisitFunction("LEN", function.Argument);
		}

		/// <summary>
		/// Visits a substring function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitSubstringFunction(SubstringFunction function)
		{
			this.CommandText.Append("SUBSTRING(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string remove function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringRemoveFunction(StringRemoveFunction function)
		{
			this.CommandText.Append("STUFF(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(", '')");
		}

		/// <summary>
		/// Visits a string character index function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCharIndexFunction(StringIndexFunction function)
		{
			this.CommandText.Append("(");
			if (function.StartIndex != null)
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument, function.StartIndex);
			}
			else
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument);
			}
			this.CommandText.Append(" - 1)");
		}

		/// <summary>
		/// Visits a string to upper function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToUpperFunction(StringToUpperFunction function)
		{
			VisitFunction("UPPER", function.Argument);
		}

		/// <summary>
		/// Visits a string to lower function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToLowerFunction(StringToLowerFunction function)
		{
			VisitFunction("LOWER", function.Argument);
		}

		/// <summary>
		/// Visits a string replace function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringReplaceFunction(StringReplaceFunction function)
		{
			VisitFunction("REPLACE", function.Argument, function.OldValue, function.NewValue);
		}

		/// <summary>
		/// Visits a string trim function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringTrimFunction(StringTrimFunction function)
		{
			this.CommandText.Append("RTRIM(LTRIM(");
			this.VisitField(function.Argument);
			this.CommandText.Append("))");
		}

		/// <summary>
		/// Visits a string compare function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCompareFunction(StringCompareFunction function)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" = ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN 0 WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" < ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN -1 ELSE 1 END)");
		}

		/// <summary>
		/// Visits a string concatenate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringConcatenateFunction(StringConcatenateFunction function)
		{
			for (var i = 0; i < function.Arguments.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(" + ");
				}
				this.VisitField(function.Arguments[i]);
			}
		}

		/// <summary>
		/// Visits a date part function.
		/// </summary>
		/// <param name="function">The function.</param>
		/// <exception cref="InvalidOperationException">Invalid date part: " + function.DatePart</exception>
		protected virtual void VisitDatePartFunction(DatePartFunction function)
		{
			switch (function.DatePart)
			{
				case DatePart.Millisecond:
				case DatePart.Second:
				case DatePart.Minute:
				case DatePart.Hour:
				case DatePart.Day:
				case DatePart.Month:
				case DatePart.Year:
				{
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
						function.Argument
					});
					break;
				}
				case DatePart.DayOfWeek:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("weekday"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.DayOfYear:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("dayofyear"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.Date:
				{
					this.CommandText.Append("DATEADD(dd, DATEDIFF(dd, 0, ");
					this.VisitField(function.Argument);
					this.CommandText.Append("), 0)");
					break;
				}
				default:
				{
					throw new InvalidOperationException("Invalid date part: " + function.DatePart);
				}
			}
		}

		/// <summary>
		/// Visits a date add function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateAddFunction(DateAddFunction function)
		{
			this.VisitFunction("DATEADD", new StatementPart[]
			{
				new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
				function.Number,
				function.Argument
			});
		}

		/// <summary>
		/// Visits a date new function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateNewFunction(DateNewFunction function)
		{
			if (function.Hour != null)
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append(") + ' ' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Hour);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Minute);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Second);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append("))");
			}

		}

		/// <summary>
		/// Visits a date difference function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateDifferenceFunction(DateDifferenceFunction function)
		{
			this.VisitFunction("DATEDIFF", function.Date1, function.Date2);
		}

		/// <summary>
		/// Visits a number absolute function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberAbsoluteFunction(NumberAbsoluteFunction function)
		{
			this.VisitFunction("ABS", function.Argument);
		}

		/// <summary>
		/// Visits a number negate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberNegateFunction(NumberNegateFunction function)
		{
			this.CommandText.Append("-");
			this.VisitField(function.Argument);
		}

		/// <summary>
		/// Visits a number ceiling function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberCeilingFunction(NumberCeilingFunction function)
		{
			this.VisitFunction("CEILING", function.Argument);
		}

		/// <summary>
		/// Visits a number floor function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberFloorFunction(NumberFloorFunction function)
		{
			this.VisitFunction("FLOOR", function.Argument);
		}

		/// <summary>
		/// Visits a number round function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRoundFunction(NumberRoundFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, function.Precision);
		}

		/// <summary>
		/// Visits a number truncate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTruncateFunction(NumberTruncateFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, new ConstantPart(0), new ConstantPart(1));
		}

		/// <summary>
		/// Visits a number sign function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberSignFunction(NumberSignFunction function)
		{
			this.VisitFunction("SIGN", function.Argument);
		}

		/// <summary>
		/// Visits a number power function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberPowerFunction(NumberPowerFunction function)
		{
			this.VisitFunction("POWER", function.Argument, function.Power);
		}

		/// <summary>
		/// Visits a number root function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRootFunction(NumberRootFunction function)
		{
			// TODO: I'm being lazy, if root > 3 then we should to convert it to POW(argument, 1 / root)
			this.VisitFunction("SQRT", function.Argument);
		}

		/// <summary>
		/// Visits a number exponential function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberExponentialFunction(NumberExponentialFunction function)
		{
			this.VisitFunction("EXP", function.Argument);
		}

		/// <summary>
		/// Visits a number log function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLogFunction(NumberLogFunction function)
		{
			this.VisitFunction("LOG", function.Argument);
		}

		/// <summary>
		/// Visits a number log10 function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLog10Function(NumberLog10Function function)
		{
			this.VisitFunction("LOG10", function.Argument);
		}

		/// <summary>
		/// Visits a number trig function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTrigFunction(NumberTrigFunction function)
		{
			if (function.Argument2 != null)
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument, function.Argument2);
			}
			else
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument);
			}
		}

		/// <summary>
		/// Visits a binary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitBinaryOperation(BinaryOperation operation)
		{
			if (operation.Operator == BinaryOperator.LeftShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" * POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else if (operation.Operator == BinaryOperator.RightShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" / POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" ");
				this.CommandText.Append(GetOperatorName(operation.Operator));
				this.CommandText.Append(" ");
				this.VisitField(operation.Right);
				this.CommandText.Append(")");
			}
		}

		private string GetOperatorName(BinaryOperator op)
		{
			switch (op)
			{
				case BinaryOperator.Add:
				{
					return "+";
				}
				case BinaryOperator.Subtract:
				{
					return "-";
				}
				case BinaryOperator.Multiply:
				{
					return "*";
				}
				case BinaryOperator.Divide:
				{
					return "/";
				}
				case BinaryOperator.Remainder:
				{
					return "%";
				}
				case BinaryOperator.ExclusiveOr:
				{
					return "^";
				}
				case BinaryOperator.LeftShift:
				{
					return "<<";
				}
				case BinaryOperator.RightShift:
				{
					return ">>";
				}
				case BinaryOperator.BitwiseAnd:
				{
					return "&";
				}
				case BinaryOperator.BitwiseOr:
				case BinaryOperator.BitwiseExclusiveOr:
				{
					return "|";
				}
				case BinaryOperator.BitwiseNot:
				{
					return "~";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a unary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitUnaryOperation(UnaryOperation operation)
		{
			this.CommandText.Append(GetOperatorName(operation.Operator));
			// TODO: If isbinary: this.Builder.Append(" ");
			this.VisitField(operation.Expression);
		}

		private string GetOperatorName(UnaryOperator op)
		{
			switch (op)
			{
				case UnaryOperator.Not:
				{
					// TODO: return IsBoolean(unary.Expression.Type) ? "NOT" : "~";
					return "NOT ";
				}
				case UnaryOperator.Negate:
				{
					return "-";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a literal part.
		/// </summary>
		/// <param name="literalPart">The literal part.</param>
		protected virtual void VisitLiteralPart(LiteralPart literalPart)
		{
			this.CommandText.Append(literalPart.Value);
		}

		/// <summary>
		/// Visits a select expression.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectExpression(SelectExpression select)
		{
			this.CommandText.Append("(");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(select.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			if (!string.IsNullOrEmpty(select.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(select.Alias);
				this.CommandText.Append("]");
			}
			this.Indent(Indentation.Outer);
		}

		private void AppendNewLine(Indentation style)
		{
			this.CommandText.AppendLine();
			this.Indent(style);
			for (var i = 0; i < this.Depth * IndentationWidth; i++)
			{
				this.CommandText.Append(" ");
			}
		}

		private void Indent(Indentation style)
		{
			if (style == Indentation.Inner)
			{
				this.Depth += 1;
			}
			else if (style == Indentation.Outer)
			{
				this.Depth -= 1;
				System.Diagnostics.Debug.Assert(this.Depth >= 0);
			}
		}
	}

	/// <summary>
	/// Converts QueryModels into Select statements for passing to the database.
	/// </summary>
	public class StatementCreator : QueryModelVisitorBase
	{
		private DatabaseMapper Configuration { get; set; }

        private bool AliasTables { get; set; }

		private SelectStatement SelectStatement { get; set; }

		private StatementCreator(DatabaseMapper mapper, bool aliasTables)
		{
			this.Configuration = mapper;
            this.AliasTables = aliasTables;
			this.SelectStatement = new SelectStatement();
		}

		/// <summary>
		/// Visits the specified query model.
		/// </summary>
		/// <param name="queryModel">The query model.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static SelectStatement Visit(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementCreator(mapper, aliasTables);
			queryModel.Accept(visitor);
			return visitor.SelectStatement;
		}

		/// <summary>
		/// Visits the statement conditions.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="conditions">The conditions.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static ConditionCollection VisitStatementConditions<T>(Expression<Func<T, bool>> conditions, DatabaseMapper mapper, bool aliasTables)
		{
			// Build a new query
			var queryParser = QueryParser.CreateDefault();
			var queryExecutor = new StatementExecutor();
			var query = new StatementQuery<T>(queryParser, queryExecutor);

			// Create an expression to select from the query with the conditions so that we have a sequence for Re-Linq to parse
			var expression = Expression.Call(
				typeof(Queryable),
				"Where",
				new Type[] { query.ElementType },
				query.Expression,
				conditions);

			// Parse the expression with Re-Linq
			var queryModel = queryParser.GetParsedQuery(expression);

			// Get the conditions from the query model
			var visitor = new StatementCreator(mapper, aliasTables);
			visitor.SelectStatement = new SelectStatement();
			queryModel.Accept(visitor);
			return visitor.SelectStatement.Conditions;
		}

		/// <summary>
		/// Visits the select clause.
		/// </summary>
		/// <param name="selectClause">The select clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
		{
			if (selectClause.Selector.NodeType != ExpressionType.Extension)
			{
				var fields = StatementPartCreator.Visit(queryModel, selectClause.Selector, this.Configuration, this.AliasTables);
				this.SelectStatement.SourceFields.Add((SourceExpression)fields);
			}

			base.VisitSelectClause(selectClause, queryModel);
		}

		/// <summary>
		/// Visits the main from clause.
		/// </summary>
		/// <param name="fromClause">From clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
		{
			if (this.Configuration.IsFunction(fromClause.ItemType))
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var functionName = this.Configuration.GetFunctionName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new UserDefinedFunction(functionName) { Alias = alias, Schema = schemaName };
			}
			else
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var tableName = this.Configuration.GetTableName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new Table(tableName) { Alias = alias, Schema = schemaName };
			}
			base.VisitMainFromClause(fromClause, queryModel);
		}

		/// <summary>
		/// Visits the join clause.
		/// </summary>
		/// <param name="joinClause">The join clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
		{
			// TODO: This seems heavy...
			// TODO: And like it's only going to deal with certain types of joins
			var table = (Table)StatementPartCreator.Visit(queryModel, joinClause.InnerSequence, this.Configuration, this.AliasTables);
            table.Alias = joinClause.ItemName.Replace("<generated>", "g");
			var leftColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.OuterKeySelector, this.Configuration, this.AliasTables);
			var rightColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.InnerKeySelector, this.Configuration, this.AliasTables);

			if (leftColumn is FieldCollection leftColumnCollection &&
				rightColumn is FieldCollection rightColumnCollection)
			{
				var joinConditions = new ConditionCollection();
				for (var i = 0; i < leftColumnCollection.Count; i++)
				{
					joinConditions.Add(new Condition(leftColumnCollection[i], SqlOperator.Equals, rightColumnCollection[i]));
				}
				this.SelectStatement.SourceJoins.Add(new Join(table, joinConditions) { JoinType = JoinType.Left });
			}
			else
			{
				this.SelectStatement.SourceJoins.Add(new Join(table, leftColumn, rightColumn) { JoinType = JoinType.Left });
			}

			base.VisitJoinClause(joinClause, queryModel, index);
		}

		/// <summary>
		/// Visits the ordering.
		/// </summary>
		/// <param name="ordering">The ordering.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="orderByClause">The order by clause.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">Invalid ordering direction: {ordering.OrderingDirection}</exception>
		public override void VisitOrdering(Ordering ordering, QueryModel queryModel, OrderByClause orderByClause, int index)
		{
			var column = (Column)StatementPartCreator.Visit(queryModel, ordering.Expression, this.Configuration, this.AliasTables);

			switch (ordering.OrderingDirection)
			{
				case OrderingDirection.Asc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Ascending));
					break;
				}
				case OrderingDirection.Desc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Descending));
					break;
				}
				default:
				{
					throw new InvalidOperationException($"Invalid ordering direction: {ordering.OrderingDirection}");
				}
			}

			base.VisitOrdering(ordering, queryModel, orderByClause, index);
		}

		/// <summary>
		/// Visits the result operator.
		/// </summary>
		/// <param name="resultOperator">The result operator.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">
		/// can't count multiple fields
		/// or
		/// can't sum multiple or no fields
		/// or
		/// can't min multiple or no fields
		/// or
		/// can't max multiple or no fields
		/// or
		/// can't average multiple or no fields
		/// </exception>
		/// <exception cref="NotSupportedException">
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// or
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// </exception>
		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
		{
			if (resultOperator is AnyResultOperator)
			{
				this.SelectStatement.IsAny = true;
				this.SelectStatement.IsAggregate = true;
				return;
			}

			if (resultOperator is AllResultOperator allResults)
			{
				this.SelectStatement.IsAll = true;
				this.SelectStatement.IsAggregate = true;
				var predicate = allResults.Predicate;
				if (predicate != null)
				{
					VisitPredicate(predicate, queryModel);
				}
				return;
			}

			if (resultOperator is ContainsResultOperator containsResult)
			{
				this.SelectStatement.IsContains = true;
				this.SelectStatement.IsAggregate = true;
				var item = containsResult.Item;
				if (item != null && item.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.ContainsItem = new ConstantPart(((ConstantExpression)item).Value);
				}
				return;
			}

			if (resultOperator is FirstResultOperator)
			{
				this.SelectStatement.Limit = 1;
				return;
			}

			if (resultOperator is LastResultOperator)
			{
				this.SelectStatement.Limit = 1;
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			if (resultOperator is CountResultOperator || resultOperator is LongCountResultOperator)
			{
				// Throw an exception if there is more than one field
				if (this.SelectStatement.SourceFields.Count > 1)
				{
					throw new InvalidOperationException("can't count multiple fields");
				}

				// Count the first field
				if (this.SelectStatement.SourceFields.Count == 0)
				{
					this.SelectStatement.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
				}
				else
				{
					this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Count, (Field)this.SelectStatement.SourceFields[0]);
				}

				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is SumResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't sum multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Sum, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MinResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't min multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Min, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MaxResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't max multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Max, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is AverageResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't average multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Average, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is DistinctResultOperator)
			{
				this.SelectStatement.IsDistinct = true;
				return;
			}

			if (resultOperator is TakeResultOperator takeResult)
			{
				var count = takeResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.Limit = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is SkipResultOperator skipResult)
			{
				var count = skipResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.StartIndex = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is ReverseResultOperator)
			{
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			base.VisitResultOperator(resultOperator, queryModel, index);
		}

		/// <summary>
		/// Visits the where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
		{
			VisitPredicate(whereClause.Predicate, queryModel);

			base.VisitWhereClause(whereClause, queryModel, index);
		}

		private void VisitPredicate(Expression predicate, QueryModel queryModel)
		{
			var whereStatement = StatementPartCreator.Visit(queryModel, predicate, this.Configuration, this.AliasTables);
			ConditionExpression condition;
			if (whereStatement is ConditionExpression conditionWhere)
			{
				condition = conditionWhere;
			}
			else if (whereStatement is UnaryOperation unaryWhere && unaryWhere.Expression is ConditionExpression unaryWhereExpression)
			{
				condition = unaryWhereExpression;
			}
			else if (whereStatement is UnaryOperation unaryWhere2 && unaryWhere2.Expression is Column)
			{
				var unary = unaryWhere2;
				var column = (Column)unary.Expression;
				condition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
			}
			else if (whereStatement is ConstantPart constantWhere && constantWhere.Value is bool booleanWhere)
			{
				condition = new Condition() {
					Field = new ConstantPart(booleanWhere),
					Operator = SqlOperator.Equals,
					Value = new ConstantPart(true)
				};
			}
			else if (whereStatement is Column columnWhere && columnWhere.PropertyType == typeof(bool))
			{
				condition = new Condition(columnWhere, SqlOperator.Equals, new ConstantPart(true));
			}
			else
			{
				throw new InvalidOperationException();
			}
			this.SelectStatement.Conditions.Add(condition);
		}
	}

	/// <summary>
	/// A dummy implementation of IQueryExecutor for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <seealso cref="IQueryExecutor" />
	internal class StatementExecutor : IQueryExecutor
	{
		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a collection query, i.e. as a query returning objects of type <typeparamref name="T" />.
		/// The query does not end with a scalar result operator, but it can end with a single result operator, for example
		/// <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" /> or <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" />. In such a case, the returned enumerable must yield exactly
		/// one object (or none if the last result operator allows empty result sets).
		/// </summary>
		/// <typeparam name="T">The type of the items returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a scalar query, i.e. as a query returning a scalar value of type <typeparamref name="T" />.
		/// The query ends with a scalar result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.CountResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SumResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the scalar value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteScalar<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a single object query, i.e. as a query returning a single object of type
		/// <typeparamref name="T" />.
		/// The query ends with a single result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the single value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <param name="returnDefaultWhenEmpty">If <see langword="true" />, the executor must return a default value when its result set is empty;
		/// if <see langword="false" />, it should throw an <see cref="T:System.InvalidOperationException" /> when its result set is empty.</param>
		/// <returns>
		/// A single value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
		{
			throw new NotImplementedException();
		}
	}

	/// <summary>
	/// Converts Expressions (such as those in Re-Linq's QueryModels) into StatementParts.
	/// </summary>
	internal class StatementPartCreator : RelinqExpressionVisitor
	{
		private QueryModel QueryModel { get; set; }

		private DatabaseMapper Configuration { get; set; }

		private bool AliasTables { get; set; }

		private Stack<StatementPart> Stack { get; set; }

		private StatementPartCreator(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			this.QueryModel = queryModel;
			this.Configuration = mapper;
			this.AliasTables = aliasTables;
			this.Stack = new Stack<StatementPart>();
		}

		public static StatementPart Visit(QueryModel queryModel, Expression expression, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementPartCreator(queryModel, mapper, aliasTables);
			visitor.Visit(expression);
			return visitor.Stack.Pop();
		}

		protected override Expression VisitBinary(BinaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				case ExpressionType.ExclusiveOr:
				{
					if (expression.Type == typeof(bool))
					{
						return VisitBinaryConditionCollection(expression);
					}
					else
					{
						return VisitBinaryOperation(expression);
					}
				}
				case ExpressionType.Equal:
				case ExpressionType.NotEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				{
					return VisitBinaryCondition(expression);
				}
				case ExpressionType.Add:
				case ExpressionType.Subtract:
				case ExpressionType.Multiply:
				case ExpressionType.Divide:
				case ExpressionType.Modulo:
				case ExpressionType.LeftShift:
				case ExpressionType.RightShift:
				{
					return VisitBinaryOperation(expression);
				}
			}

			return base.VisitBinary(expression);
		}

		private Expression VisitBinaryConditionCollection(BinaryExpression expression)
		{
			Visit(expression.Left);
			Visit(expression.Right);

			// Convert the conditions on the stack to a collection and set each condition's relationship
			var newCondition = new ConditionCollection();
			for (var i = 0; i < 2; i++)
			{
				ConditionExpression subCondition;
				if (this.Stack.Peek() is ConditionExpression)
				{
					subCondition = (ConditionExpression)this.Stack.Pop();
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp && unaryOp.Expression is ConditionExpression)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					subCondition = (ConditionExpression)unary.Expression;
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp2 && unaryOp2.Expression is Column)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					var column = (Column)unary.Expression;
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
				}
				else if (this.Stack.Peek() is ConstantPart constantPart && constantPart.Value is bool)
				{
					var constant = (ConstantPart)this.Stack.Pop();
					var value = (bool)constant.Value;
					subCondition = new Condition() {
						Field = new ConstantPart(value),
						Operator = SqlOperator.Equals,
						Value = new ConstantPart(true)
					};
				}
				else if (this.Stack.Peek() is Column columnPart && columnPart.PropertyType == typeof(bool))
				{
					var column = (Column)this.Stack.Pop();
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(true));
				}
				else
				{
					break;
				}

				if (subCondition != null)
				{
					newCondition.Insert(0, subCondition);

					if (expression.NodeType == ExpressionType.And ||
						expression.NodeType == ExpressionType.AndAlso)
					{
						subCondition.Relationship = ConditionRelationship.And;
					}
					else
					{
						subCondition.Relationship = ConditionRelationship.Or;
					}
				}
			}

			if (newCondition.Count > 1)
			{
				this.Stack.Push(newCondition);
			}
			else
			{
				this.Stack.Push(newCondition[0]);
			}

			return expression;
		}

		private Expression VisitBinaryCondition(BinaryExpression expression)
		{
			var newCondition = new Condition();
			Visit(expression.Left);
			newCondition.Field = this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Equal:
				{
					newCondition.Operator = SqlOperator.Equals;
					break;
				}
				case ExpressionType.NotEqual:
				{
					newCondition.Operator = SqlOperator.NotEquals;
					break;
				}
				case ExpressionType.LessThan:
				{
					newCondition.Operator = SqlOperator.IsLessThan;
					break;
				}
				case ExpressionType.LessThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsLessThanOrEqualTo;
					break;
				}
				case ExpressionType.GreaterThan:
				{
					newCondition.Operator = SqlOperator.IsGreaterThan;
					break;
				}
				case ExpressionType.GreaterThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsGreaterThanOrEqualTo;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newCondition.Value = this.Stack.Pop();

			if (newCondition.Field.PartType == StatementPartType.FieldCollection)
			{
				// If anonymous types have been passed in for multi-value checking, we need to split
				// them out manually from the field collection and constant part that Relinq creates
				var fields = (FieldCollection)newCondition.Field;
				var value = ((ConstantPart)newCondition.Value).Value;
				var valueList = value.GetType().GetProperties().Select(x => x.GetValue(value, null)).ToList();
				var newConditionCollection = new ConditionCollection();
				// Swap the operator if it's NotEquals
				var op = newCondition.Operator;
				if (op == SqlOperator.NotEquals)
				{
					op = SqlOperator.Equals;
					newConditionCollection.Not = true;
				}
				for (var i = 0; i < fields.Count; i++)
				{
					newConditionCollection.Add(new Condition(fields[i], op, valueList[i]));
				}
				this.Stack.Push(newConditionCollection);
			}
			else
			{
				this.Stack.Push(newCondition);
			}

			return expression;
		}

		private Expression VisitBinaryOperation(BinaryExpression expression)
		{
			var newBinary = new BinaryOperation();
			Visit(expression.Left);
			newBinary.Left = (SourceExpression)this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Add:
				{
					newBinary.Operator = BinaryOperator.Add;
					break;
				}
				case ExpressionType.Subtract:
				{
					newBinary.Operator = BinaryOperator.Subtract;
					break;
				}
				case ExpressionType.Multiply:
				{
					newBinary.Operator = BinaryOperator.Multiply;
					break;
				}
				case ExpressionType.Divide:
				{
					newBinary.Operator = BinaryOperator.Divide;
					break;
				}
				case ExpressionType.Modulo:
				{
					newBinary.Operator = BinaryOperator.Remainder;
					break;
				}
				case ExpressionType.LeftShift:
				{
					newBinary.Operator = BinaryOperator.LeftShift;
					break;
				}
				case ExpressionType.RightShift:
				{
					newBinary.Operator = BinaryOperator.RightShift;
					break;
				}
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				{
					newBinary.Operator = BinaryOperator.BitwiseAnd;
					break;
				}
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				{
					newBinary.Operator = BinaryOperator.BitwiseOr;
					break;
				}
				case ExpressionType.ExclusiveOr:
				{
					newBinary.Operator = BinaryOperator.ExclusiveOr;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newBinary.Right = (SourceExpression)this.Stack.Pop();
			this.Stack.Push(newBinary);

			return expression;
		}

		protected override Expression VisitConditional(ConditionalExpression node)
		{
			var newConditionalCase = new ConditionalCase();
			Visit(node.Test);
			newConditionalCase.Test = this.Stack.Pop();
			Visit(node.IfTrue);
			newConditionalCase.IfTrue = this.Stack.Pop();
			Visit(node.IfFalse);
			newConditionalCase.IfFalse = this.Stack.Pop();
			this.Stack.Push(newConditionalCase);
			return node;
		}

		protected override Expression VisitConstant(ConstantExpression expression)
		{
			if (expression.Value == null)
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			else if (this.Configuration.ShouldMapType(expression.Type))
			{
				var primaryKeyName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
				var property = expression.Value.GetType().GetProperty(primaryKeyName);
				var value = property.GetValue(expression.Value);
				this.Stack.Push(new ConstantPart(value));
			}
			else if (TypeHelper.IsGenericType(expression.Type, typeof(IQueryable<>)))
			{
				var queryType = expression.Value.GetType().GetGenericArguments()[0];
				var tableName = this.Configuration.GetTableName(queryType);
				this.Stack.Push(new Table(tableName));
			}
			else
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			return expression;
		}

		protected override Expression VisitMember(MemberExpression expression)
		{
			if (expression.Member.DeclaringType == typeof(string))
			{
				switch (expression.Member.Name)
				{
					case "Length":
					{
						var newFunction = new StringLengthFunction();
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.DeclaringType == typeof(DateTime) || expression.Member.DeclaringType == typeof(DateTimeOffset))
			{
				switch (expression.Member.Name)
				{
					case "Date":
					{
						var newFunction = new DatePartFunction(DatePart.Date);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Day":
					{
						var newFunction = new DatePartFunction(DatePart.Day);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Month":
					{
						var newFunction = new DatePartFunction(DatePart.Month);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Year":
					{
						var newFunction = new DatePartFunction(DatePart.Year);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Hour":
					{
						var newFunction = new DatePartFunction(DatePart.Hour);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Minute":
					{
						var newFunction = new DatePartFunction(DatePart.Minute);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Second":
					{
						var newFunction = new DatePartFunction(DatePart.Second);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Millisecond":
					{
						var newFunction = new DatePartFunction(DatePart.Millisecond);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfWeek":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfWeek);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfYear":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfYear);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.MemberType == MemberTypes.Property)
			{
				string tableName;
				if (this.AliasTables)
				{
					if (expression.Expression is UnaryExpression unaryExpression)
					{
						var source = (QuerySourceReferenceExpression)unaryExpression.Operand;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else if (expression.Expression is MemberExpression memberExpression)
					{
						var source = (QuerySourceReferenceExpression)memberExpression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else
					{
						var source = (QuerySourceReferenceExpression)expression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
				}
				else
				{
					// The property may be declared on a base type, so we can't just get DeclaringType
					// Instead, we get the type from the expression that was used to reference it
					var propertyType = expression.Expression.Type;

					// HACK: Replace interfaces with actual tables
					//	There has to be a way of intercepting the QueryModel creation??
					if (propertyType.IsInterface)
					{
						propertyType = this.QueryModel.MainFromClause.ItemType;
					}

					tableName = this.Configuration.GetTableName(propertyType);
				}

				var property = (PropertyInfo)expression.Member;
				var columnName = this.Configuration.GetColumnName(property);
				if (this.Configuration.IsRelatedItem(property))
				{
					// TODO: Should this be done here, or when converting the statement to SQL?
					columnName = this.Configuration.GetForeignKeyColumnName(property);
				}
				var newColumn = new Column(tableName, columnName) { PropertyType = property.PropertyType };
				this.Stack.Push(newColumn);
				return expression;
			}

			throw new NotSupportedException($"The member access '{expression.Member}' is not supported");
		}

		protected override Expression VisitMethodCall(MethodCallExpression expression)
		{
			var handled = false;

			if (expression.Method.DeclaringType == typeof(string))
			{
				handled = VisitStringMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(DateTime))
			{
				handled = VisitDateTimeMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(decimal))
			{
				handled = VisitDecimalMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(Math))
			{
				handled = VisitMathMethodCall(expression);
			}

			if (!handled)
			{
				if (expression.Method.Name == "ToString")
				{
					handled = VisitToStringMethodCall(expression);
				}
				else if (expression.Method.Name == "Equals")
				{
					handled = VisitEqualsMethodCall(expression);
				}
				else if (!expression.Method.IsStatic && expression.Method.Name == "CompareTo" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 1)
				{
					handled = VisitCompareToMethodCall(expression);
				}
				else if (expression.Method.IsStatic && expression.Method.Name == "Compare" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 2)
				{
					handled = VisitCompareMethodCall(expression);
				}
			}

			return handled ? expression : base.VisitMethodCall(expression);
		}

		private bool VisitStringMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "StartsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.StartsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "EndsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.EndsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Contains":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.Contains;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Concat":
				{
					var newFunction = new StringConcatenateFunction();
					IList<Expression> args = expression.Arguments;
					if (args.Count == 1 && args[0].NodeType == ExpressionType.NewArrayInit)
					{
						args = ((NewArrayExpression)args[0]).Expressions;
					}
					for (var i = 0; i < args.Count; i++)
					{
						this.Visit(args[i]);
						newFunction.Arguments.Add(this.Stack.Pop());
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IsNullOrEmpty":
				{
					var newCondition = new ConditionCollection();

					var isNullCondition = new Condition();
					this.Visit(expression.Arguments[0]);
					isNullCondition.Field = this.Stack.Pop();
					isNullCondition.Operator = SqlOperator.Equals;
					isNullCondition.Value = new ConstantPart(null);
					newCondition.Add(isNullCondition);

					var notEqualsCondition = new Condition();
					notEqualsCondition.Relationship = ConditionRelationship.Or;
					this.Visit(expression.Arguments[0]);
					notEqualsCondition.Field = this.Stack.Pop();
					notEqualsCondition.Operator = SqlOperator.Equals;
					notEqualsCondition.Value = new ConstantPart("");
					newCondition.Add(notEqualsCondition);

					this.Stack.Push(newCondition);
					return true;
				}
				case "ToUpper":
				case "ToUpperInvariant":
				{
					var newFunction = new StringToUpperFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "ToLower":
				case "ToLowerInvariant":
				{
					var newFunction = new StringToLowerFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Replace":
				{
					var newFunction = new StringReplaceFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.OldValue = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.NewValue = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Substring":
				{
					var newFunction = new SubstringFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Remove":
				{
					var newFunction = new StringRemoveFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IndexOf":
				{
					var newFunction = new StringIndexFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StringToFind = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.StartIndex = this.Stack.Pop();
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Trim":
				{
					var newFunction = new StringTrimFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDateTimeMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "op_Subtract":
				{
					if (expression.Arguments[1].Type == typeof(DateTime))
					{
						var newFunction = new DateDifferenceFunction();
						this.Visit(expression.Arguments[0]);
						newFunction.Date1 = this.Stack.Pop();
						this.Visit(expression.Arguments[1]);
						newFunction.Date2 = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return true;
					}
					break;
				}
				case "AddDays":
				{
					var newFunction = new DateAddFunction(DatePart.Day);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMonths":
				{
					var newFunction = new DateAddFunction(DatePart.Month);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddYears":
				{
					var newFunction = new DateAddFunction(DatePart.Year);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddHours":
				{
					var newFunction = new DateAddFunction(DatePart.Hour);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMinutes":
				{
					var newFunction = new DateAddFunction(DatePart.Minute);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddSeconds":
				{
					var newFunction = new DateAddFunction(DatePart.Second);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMilliseconds":
				{
					var newFunction = new DateAddFunction(DatePart.Millisecond);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDecimalMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Add":
				case "Subtract":
				case "Multiply":
				case "Divide":
				case "Remainder":
				{
					var newOperation = new BinaryOperation();
					this.Visit(expression.Arguments[0]);
					newOperation.Left = (SourceExpression)this.Stack.Pop();
					newOperation.Operator = (BinaryOperator)Enum.Parse(typeof(BinaryOperator), expression.Method.Name);
					this.Visit(expression.Arguments[1]);
					newOperation.Right = (SourceExpression)this.Stack.Pop();
					this.Stack.Push(newOperation);
					return true;
				}
				case "Negate":
				{
					var newFunction = new NumberNegateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Compare":
				{
					this.Visit(Expression.Condition(
						Expression.Equal(expression.Arguments[0], expression.Arguments[1]),
						Expression.Constant(0),
						Expression.Condition(
							Expression.LessThan(expression.Arguments[0], expression.Arguments[1]),
							Expression.Constant(-1),
							Expression.Constant(1)
							)));
					return true;
				}
			}

			return false;
		}

		private bool VisitMathMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Log":
				{
					var newFunction = new NumberLogFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Log10":
				{
					var newFunction = new NumberLog10Function();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sign":
				{
					var newFunction = new NumberSignFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Exp":
				{
					var newFunction = new NumberExponentialFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sqrt":
				{
					var newFunction = new NumberRootFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					newFunction.Root = new ConstantPart(2);
					this.Stack.Push(newFunction);
					return true;
				}
				case "Pow":
				{
					var newFunction = new NumberPowerFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.Power = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Abs":
				{
					var newFunction = new NumberAbsoluteFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sin":
				case "Cos":
				case "Tan":
				case "Acos":
				case "Asin":
				case "Atan":
				case "Atan2":
				{
					var newFunction = new NumberTrigFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Argument2 = this.Stack.Pop();
					}
					newFunction.Function = (TrigFunction)Enum.Parse(typeof(TrigFunction), expression.Method.Name);
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitToStringMethodCall(MethodCallExpression expression)
		{
			if (expression.Object.Type == typeof(string))
			{
				this.Visit(expression.Object);
			}
			else
			{
				var newFunction = new ConvertFunction();
				this.Visit(expression.Arguments[0]);
				newFunction.Expression = (SourceExpression)this.Stack.Pop();
				this.Stack.Push(newFunction);
			}
			return true;
		}

		private bool VisitEqualsMethodCall(MethodCallExpression expression)
		{
			var condition = new Condition();
			condition.Operator = SqlOperator.Equals;
			if (expression.Method.IsStatic && expression.Method.DeclaringType == typeof(object))
			{
				this.Visit(expression.Arguments[0]);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[1]);
				condition.Value = this.Stack.Pop();
			}
			else if (!expression.Method.IsStatic && expression.Arguments.Count > 0 && expression.Arguments[0].Type == expression.Object.Type)
			{
				// TODO: Get the other arguments, most importantly StringComparison
				this.Visit(expression.Object);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[0]);
				condition.Value = this.Stack.Pop();
			}
			this.Stack.Push(condition);
			return true;
		}

		private bool VisitCompareToMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Object);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[0]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		private bool VisitCompareMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Arguments[0]);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[1]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		protected override Expression VisitNew(NewExpression expression)
		{
			if (expression.Type == typeof(DateTime))
			{
				// It's a date, so put its arguments into a DateNewFunction
				var function = new DateNewFunction();
				if (expression.Arguments.Count == 3)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
				}
				else if (expression.Arguments.Count == 6)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
					this.Visit(expression.Arguments[3]);
					function.Hour = this.Stack.Pop();
					this.Visit(expression.Arguments[4]);
					function.Minute = this.Stack.Pop();
					this.Visit(expression.Arguments[5]);
					function.Second = this.Stack.Pop();
				}
				this.Stack.Push(function);
				return expression;
			}
			else if (expression.Arguments.Count > 0)
			{
				// It's a new anonymous object, so get its properties as columns
				var fields = new FieldCollection();
				foreach (var argument in expression.Arguments)
				{
					this.Visit(argument);
					fields.Add((SourceExpression)this.Stack.Pop());
				}
				this.Stack.Push(fields);
				return expression;
			}

			return base.VisitNew(expression);
		}

		protected override Expression VisitUnary(UnaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.Not:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Not;
					Visit(expression.Operand);

					newOperation.Expression = this.Stack.Pop();
					// Push the condition onto the stack instead
					if (newOperation.Expression is Condition newCondition)
					{
						newCondition.Not = true;
						this.Stack.Push(newCondition);
					}
					else
					{
						this.Stack.Push(newOperation);
					}
					return expression;
				}
				case ExpressionType.Negate:
				case ExpressionType.NegateChecked:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Negate;
					Visit(expression.Operand);
					newOperation.Expression = this.Stack.Pop();
					this.Stack.Push(newOperation);
					return expression;
				}
				case ExpressionType.UnaryPlus:
				{
					Visit(expression.Operand);
					return expression;
				}
				case ExpressionType.Convert:
				{
					// Ignore conversions for now
					Visit(expression.Operand);
					return expression;
				}
				default:
				{
					throw new NotSupportedException($"The unary operator '{expression.NodeType}' is not supported");
				}
			}
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
		{
			var tableName = expression.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
			var columnName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
			var newColumn = new Column(tableName, columnName);
			this.Stack.Push(newColumn);

			return base.VisitQuerySourceReference(expression);
		}

		protected override Expression VisitSubQuery(SubQueryExpression expression)
		{
			if (expression.QueryModel.ResultOperators.Count > 0 &&
				expression.QueryModel.ResultOperators[0] is Remotion.Linq.Clauses.ResultOperators.ContainsResultOperator contains)
			{
				// It's an Array.Contains, so we need to convert the subquery into a condition
				var newCondition = new Condition();
				newCondition.Operator = SqlOperator.IsIn;

				Visit(contains.Item);
				newCondition.Field = this.Stack.Pop();

				if (TypeHelper.IsGenericType(expression.QueryModel.MainFromClause.FromExpression.Type, typeof(IQueryable<>)))
				{
					// Create the sub-select statement
					var subselect = StatementCreator.Visit(expression.QueryModel, this.Configuration, true);
					subselect.IsContains = false;
					if (subselect.SourceFields.Count == 0)
					{
						var subselectField = expression.QueryModel.SelectClause.Selector;
						Visit(subselectField);
						subselect.SourceFields.Add((SourceExpression)this.Stack.Pop());
					}
					newCondition.Value = subselect;
				}
				else
				{
					// Just check in the array that was passed
					Visit(expression.QueryModel.MainFromClause.FromExpression);
					newCondition.Value = this.Stack.Pop();
				}

				this.Stack.Push(newCondition);
			}

			return base.VisitSubQuery(expression);
		}

#if DEBUG

		// NOTE: I got sick of re-adding these everytime I wanted to figure out what was going on, so
		// I'm leaving them here in debug only

		protected override Expression VisitBlock(BlockExpression node)
		{
			BreakpointHook();
			return base.VisitBlock(node);
		}

		protected override CatchBlock VisitCatchBlock(CatchBlock node)
		{
			BreakpointHook();
			return base.VisitCatchBlock(node);
		}

		protected override Expression VisitDebugInfo(DebugInfoExpression node)
		{
			BreakpointHook();
			return base.VisitDebugInfo(node);
		}

		protected override Expression VisitDefault(DefaultExpression node)
		{
			BreakpointHook();
			return base.VisitDefault(node);
		}

		protected override Expression VisitDynamic(DynamicExpression node)
		{
			BreakpointHook();
			return base.VisitDynamic(node);
		}

		protected override ElementInit VisitElementInit(ElementInit node)
		{
			BreakpointHook();
			return base.VisitElementInit(node);
		}

		protected override Expression VisitExtension(Expression node)
		{
			BreakpointHook();
			return base.VisitExtension(node);
		}

		protected override Expression VisitGoto(GotoExpression node)
		{
			BreakpointHook();
			return base.VisitGoto(node);
		}

		protected override Expression VisitIndex(IndexExpression node)
		{
			BreakpointHook();
			return base.VisitIndex(node);
		}

		protected override Expression VisitInvocation(InvocationExpression node)
		{
			BreakpointHook();
			return base.VisitInvocation(node);
		}

		protected override Expression VisitLabel(LabelExpression node)
		{
			BreakpointHook();
			return base.VisitLabel(node);
		}

		protected override LabelTarget VisitLabelTarget(LabelTarget node)
		{
			BreakpointHook();
			return base.VisitLabelTarget(node);
		}

		protected override Expression VisitLambda<T>(Expression<T> node)
		{
			BreakpointHook();
			return base.VisitLambda(node);
		}

		protected override Expression VisitListInit(ListInitExpression node)
		{
			BreakpointHook();
			return base.VisitListInit(node);
		}

		protected override Expression VisitLoop(LoopExpression node)
		{
			BreakpointHook();
			return base.VisitLoop(node);
		}

		protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
		{
			BreakpointHook();
			return base.VisitMemberAssignment(node);
		}

		protected override MemberBinding VisitMemberBinding(MemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberBinding(node);
		}

		protected override Expression VisitMemberInit(MemberInitExpression node)
		{
			BreakpointHook();
			return base.VisitMemberInit(node);
		}

		protected override MemberListBinding VisitMemberListBinding(MemberListBinding node)
		{
			BreakpointHook();
			return base.VisitMemberListBinding(node);
		}

		protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberMemberBinding(node);
		}

		protected override Expression VisitNewArray(NewArrayExpression node)
		{
			BreakpointHook();
			return base.VisitNewArray(node);
		}

		protected override Expression VisitParameter(ParameterExpression node)
		{
			BreakpointHook();
			return base.VisitParameter(node);
		}

		protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
		{
			BreakpointHook();
			return base.VisitRuntimeVariables(node);
		}

		protected override Expression VisitSwitch(SwitchExpression node)
		{
			BreakpointHook();
			return base.VisitSwitch(node);
		}

		protected override SwitchCase VisitSwitchCase(SwitchCase node)
		{
			BreakpointHook();
			return base.VisitSwitchCase(node);
		}

		protected override Expression VisitTry(TryExpression node)
		{
			BreakpointHook();
			return base.VisitTry(node);
		}

		protected override Expression VisitTypeBinary(TypeBinaryExpression node)
		{
			BreakpointHook();
			return base.VisitTypeBinary(node);
		}

		// When creating statement parts, put a breakpoint here if you would like to debug
		protected void BreakpointHook()
		{
		}
#endif
	}

	/// <summary>
	/// A dummy implementation of QueryableBase for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso cref="Remotion.Linq.QueryableBase{T}" />
	internal class StatementQuery<T> : QueryableBase<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="queryParser">The <see cref="T:Remotion.Linq.Parsing.Structure.IQueryParser" /> used to parse queries. Specify an instance of
		/// <see cref="T:Remotion.Linq.Parsing.Structure.QueryParser" /> for default behavior. See also <see cref="M:Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault" />.</param>
		/// <param name="executor">The <see cref="T:Remotion.Linq.IQueryExecutor" /> used to execute the query represented by this <see cref="T:Remotion.Linq.QueryableBase`1" />.</param>
		public StatementQuery(IQueryParser queryParser, IQueryExecutor executor)
			: base(new DefaultQueryProvider(typeof(StatementQuery<>), queryParser, executor))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
		/// <param name="expression">The expression.</param>
		public StatementQuery(IQueryProvider provider, Expression expression)
			: base(provider, expression)
		{
		}
	}

	/// <summary>
	/// Contains helper methods for dealing with types.
	/// </summary>
	public static class TypeHelper
	{
		///// <summary>
		///// Finds any interfaces of type IEnumerable on a type.
		///// </summary>
		///// <param name="sequenceType">The type to search for IEnumerable.</param>
		///// <returns></returns>
		//public static Type FindIEnumerable(Type sequenceType)
		//{
		//	if (sequenceType == null || sequenceType == typeof(string))
		//	{
		//		return null;
		//	}

		//	if (sequenceType.IsArray)
		//	{
		//		return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
		//	}

		//	if (sequenceType.IsGenericType)
		//	{
		//		foreach (Type arg in sequenceType.GetGenericArguments())
		//		{
		//			Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
		//			if (ienum.IsAssignableFrom(sequenceType))
		//			{
		//				return ienum;
		//			}
		//		}
		//	}

		//	Type[] interfaces = sequenceType.GetInterfaces();
		//	if (interfaces != null && interfaces.Length > 0)
		//	{
		//		foreach (Type iface in interfaces)
		//		{
		//			Type ienum = FindIEnumerable(iface);
		//			if (ienum != null)
		//				return ienum;
		//		}
		//	}

		//	if (sequenceType.BaseType != null && sequenceType.BaseType != typeof(object))
		//	{
		//		return FindIEnumerable(sequenceType.BaseType);
		//	}

		//	return null;
		//}

		///// <summary>
		///// Gets the type of element contained in a sequence.
		///// </summary>
		///// <param name="sequenceType">The type of the sequence, which must implement an IEnumerable interface.</param>
		///// <returns></returns>
		//public static Type GetElementType(Type sequenceType)
		//{
		//	Type enumerableType = FindIEnumerable(sequenceType);
		//	if (enumerableType == null)
		//	{
		//		return sequenceType;
		//	}
		//	else
		//	{
		//		return enumerableType.GetGenericArguments()[0];
		//	}
		//}

		/// <summary>
		/// Determines whether the specified type is nullable.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns><c>true</c> if the specified type is nullable; otherwise, <c>false</c>.</returns>
		public static bool IsNullableType(Type type)
		{
			return type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		/// <summary>
		/// Gets a non-nullable version of the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public static Type GetNonNullableType(Type type)
		{
			if (IsNullableType(type))
			{
				return type.GetGenericArguments()[0];
			}
			return type;
		}

		///// <summary>
		///// Determines whether the specified type is boolean.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is boolean; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsBoolean(Type type)
		//{
		//	return Type.GetTypeCode(type) == TypeCode.Boolean;
		//}

		//public static bool IsInteger(Type type)
		//{
		//	Type nnType = GetNonNullableType(type);
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.SByte:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.Byte:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		///// <summary>
		///// Determines whether the specified type is numeric.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is numeric; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsNumeric(Type type)
		//{
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.Byte:
		//		case TypeCode.Decimal:
		//		case TypeCode.Double:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.SByte:
		//		case TypeCode.Single:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		//public static bool IsAnonymous(Type type)
		//{
		//	// From http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
		//	// HACK: The only way to detect anonymous types right now.
		//	return
		//		Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false) &&
		//		type.IsGenericType &&
		//		type.Name.Contains("AnonymousType") &&
		//		(type.Name.StartsWith("<>") || type.Name.StartsWith("VB$")) &&
		//		(type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
		//}

		public static bool IsGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/982487/testing-if-object-is-of-generic-type-in-c-sharp
			while (type != null)
			{
				if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
				if (genericType.IsInterface && IsAssignableToGenericType(type, genericType))
				{
					return true;
				}
				type = type.BaseType;
			}
			return false;
		}

		public static bool IsAssignableToGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/5461295/using-isassignablefrom-with-generics
			var interfaceTypes = type.GetInterfaces();

			foreach (var it in interfaceTypes)
			{
				if (it.IsGenericType && it.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
			}

			if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
			{
				return true;
			}

			var baseType = type.BaseType;
			if (baseType == null)
			{
				return false;
			}

			return IsAssignableToGenericType(baseType, genericType);
		}

		///// <summary>
		///// Returns an object of the specified type and whose value is equivalent to the specified object.
		///// </summary>
		///// <param name="value">An object that implements the System.IConvertible interface.</param>
		///// <param name="conversionType">The type of object to return.</param>
		///// <returns>
		///// An object whose type is conversionType and whose value is equivalent to value.-or-A
		///// null reference (Nothing in Visual Basic), if value is null and conversionType
		///// is not a value type.
		///// </returns>
		//public static object ChangeType(object value, Type conversionType)
		//{
		//	if (value == null || value == DBNull.Value)
		//	{
		//		// TODO: Maybe not...
		//		// It would be better to make this generic and pass in the default value
		//		// But that would involve changing emitted code
		//		return null;
		//	}

		//	Type safeType = Nullable.GetUnderlyingType(conversionType) ?? conversionType;
		//	if (safeType.IsEnum)
		//	{
		//		return Enum.ToObject(safeType, value);
		//	}
		//	else
		//	{
		//		return Convert.ChangeType(value, safeType);
		//	}
		//}
	}

	/// <summary>
	/// An aggregate operation (such as sum or count) on a source field.
	/// </summary>
	public sealed class Aggregate : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Aggregate;
			}
		}

		/// <summary>
		/// Gets or sets a value indicating whether this aggregate operation is distinct.
		/// </summary>
		/// <value>
		/// <c>true</c> if this aggregate operation is distinct; otherwise, <c>false</c>.
		/// </value>
		public bool IsDistinct { get; set; }

		/// <summary>
		/// Gets or sets the type of the aggregate (e.g. sum, or count).
		/// </summary>
		/// <value>
		/// The type of the aggregate.
		/// </value>
		public AggregateType AggregateType { get; internal set; }

		/// <summary>
		/// Gets or sets the field to be aggregated.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public Field Field { get; internal set; }

		// TODO: Remove all of the internal empty constructors
		/// <summary>
		/// Initializes a new instance of the <see cref="Aggregate" /> class.
		/// </summary>
		internal Aggregate()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Aggregate" /> class.
		/// </summary>
		/// <param name="aggregateType">The type of the aggregate (e.g. sum, or count).</param>
		/// <param name="field">The field to be aggregated.</param>
		public Aggregate(AggregateType aggregateType, Field field)
		{
			this.AggregateType = aggregateType;
			this.Field = field;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.AggregateType.ToString());
			b.Append("(");
			if (this.IsDistinct)
			{
				b.Append("DISTINCT ");
			}
			if (this.Field != null)
			{
				b.Append(this.Field.ToString());
			}
			else
			{
				b.Append("ALL");
			}
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// An aggregate that may be applied to a field such as sum or count.
	/// </summary>
	public enum AggregateType
	{
		/// <summary>
		/// No aggregate.
		/// </summary>
		None,
		/// <summary>
		/// Counts the number of items.
		/// </summary>
		Count,
		/// <summary>
		/// Counts the number of items and returns a large integer.
		/// </summary>
		BigCount,
		/// <summary>
		/// Adds the values contained in the field together.
		/// </summary>
		Sum,
		/// <summary>
		/// Returns the minimum value contained in the field.
		/// </summary>
		Min,
		/// <summary>
		/// Returns the maximum value contained in the field.
		/// </summary>
		Max,
		/// <summary>
		/// Returns the average value contained in the field.
		/// </summary>
		Average,
	}

	/// <summary>
	/// A class for replacing parameters in an expression.
	/// </summary>
	/// <remarks>
	/// This class is used to consolidate anonymous parameters when combining lambda expressions, so
	/// that all of the parameters have the same object reference.
	/// </remarks>
	internal sealed class AnonymousParameterReplacer : ExpressionVisitor
	{
		private readonly ReadOnlyCollection<ParameterExpression> _parameters;

		/// <summary>
		/// Prevents a default instance of the <see cref="AnonymousParameterReplacer" /> class from being created.
		/// </summary>
		/// <param name="parameters">The parameters.</param>
		private AnonymousParameterReplacer(ReadOnlyCollection<ParameterExpression> parameters)
		{
			_parameters = parameters;
		}

		/// <summary>
		/// Replaces the parameters in an expression with the supplied parameters.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns></returns>
		public static Expression Replace(Expression expression, ReadOnlyCollection<ParameterExpression> parameters)
		{
			return new AnonymousParameterReplacer(parameters).Visit(expression);
		}

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.ParameterExpression" />.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitParameter(ParameterExpression node)
		{
			foreach (var parameter in _parameters)
			{
				if (parameter.Type == node.Type)
				{
					return parameter;
				}
			}
			return node;
		}
	}

	/// <summary>
	/// An operation with a binary operator e.g. 1 + 2.
	/// </summary>
	public sealed class BinaryOperation : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.BinaryOperation;
			}
		}

		/// <summary>
		/// Gets or sets the expression on the left of the operator.
		/// </summary>
		/// <value>
		/// The left expression.
		/// </value>
		public SourceExpression Left { get; internal set; }

		/// <summary>
		/// Gets or sets the operator.
		/// </summary>
		/// <value>
		/// The operator.
		/// </value>
		public BinaryOperator Operator { get; internal set; }

		private string OperatorString
		{
			get
			{
				switch (this.Operator)
				{
					case BinaryOperator.Add:
					{
						return "+";
					}
					case BinaryOperator.Subtract:
					{
						return "-";
					}
					case BinaryOperator.Multiply:
					{
						return "*";
					}
					case BinaryOperator.Divide:
					{
						return "/";
					}
					case BinaryOperator.Remainder:
					{
						return "%";
					}
					case BinaryOperator.ExclusiveOr:
					{
						return "^";
					}
					case BinaryOperator.LeftShift:
					{
						return "<<";
					}
					case BinaryOperator.RightShift:
					{
						return ">>";
					}
					case BinaryOperator.BitwiseAnd:
					{
						return "&";
					}
					case BinaryOperator.BitwiseOr:
					{
						return "|";
					}
					case BinaryOperator.BitwiseExclusiveOr:
					{
						return "^";
					}
					default:
					{
						throw new InvalidOperationException("Invalid Operator: " + this.Operator);
					}
				}
			}
		}

		/// <summary>
		/// Gets or sets the expression on the right of the operator.
		/// </summary>
		/// <value>
		/// The right expression.
		/// </value>
		public SourceExpression Right { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="BinaryOperation" /> class.
		/// </summary>
		internal BinaryOperation()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="BinaryOperation" /> class.
		/// </summary>
		/// <param name="left">The expression on the left of the operator.</param>
		/// <param name="op">The operator.</param>
		/// <param name="right">The expression on the right of the operator.</param>
		public BinaryOperation(SourceExpression left, BinaryOperator op, SourceExpression right)
		{
			this.Left = left;
			this.Operator = op;
			this.Right = right;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return "(" + this.Left.ToString() + " " + this.OperatorString + " " + this.Right.ToString() + ")";
		}
	}

	/// <summary>
	/// An operator that is performed on two expressions.
	/// </summary>
	public enum BinaryOperator
	{
		/// <summary>
		/// Add the expressions together.
		/// </summary>
		Add,
		/// <summary>
		/// Subtract the right expression from the left.
		/// </summary>
		Subtract,
		/// <summary>
		/// Multiply the expressions together.
		/// </summary>
		Multiply,
		/// <summary>
		/// Divide the left expression by the right.
		/// </summary>
		Divide,
		/// <summary>
		/// Divide the left expression by the right and return the remainder.
		/// </summary>
		Remainder,
		/// <summary>
		/// Perform an exclusive OR operation on the expressions.
		/// </summary>
		ExclusiveOr,
		/// <summary>
		/// Perform a left shift operation on the expressions.
		/// </summary>
		LeftShift,
		/// <summary>
		/// Perform a right shift operation on the expressions.
		/// </summary>
		RightShift,
		/// <summary>
		/// Perform a bitwise AND operation on the expressions.
		/// </summary>
		BitwiseAnd,
		/// <summary>
		/// Perform a bitwise OR operation on the expressions.
		/// </summary>
		BitwiseOr,
		/// <summary>
		/// Perform a bitwise exclusive OR operation on the expressions.
		/// </summary>
		BitwiseExclusiveOr,
		/// <summary>
		/// Perform a bitwise NOT operation on the expressions.
		/// </summary>
		BitwiseNot,
	}

	/// <summary>
	/// Returns the first non-null expression.
	/// </summary>
	public sealed class CoalesceFunction : Field
	{

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.CoalesceFunction;
			}
		}

		/// <summary>
		/// Gets or sets the first expression.
		/// </summary>
		/// <value>
		/// The first expression.
		/// </value>
		public List<SourceExpression> Arguments { get; } = new List<SourceExpression>();

		/// <summary>
		/// Initializes a new instance of the <see cref="CoalesceFunction" /> class.
		/// </summary>
		internal CoalesceFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CoalesceFunction" /> class.
		/// </summary>
		/// <param name="arguments">The arguments.</param>
		public CoalesceFunction(params SourceExpression[] arguments)
		{
			this.Arguments.AddRange(arguments);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("COALESCE(");
			b.Append(string.Join(", ", this.Arguments.Select(a => a.ToString())));
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// A column in a table.
	/// </summary>
	public sealed class Column : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Column;
			}
		}

		/// <summary>
		/// Gets or sets the table.
		/// </summary>
		/// <value>
		/// The table.
		/// </value>
		public Table Table { get; set; }

		/// <summary>
		/// Gets the name of the column.
		/// </summary>
		/// <value>
		/// The name of the column.
		/// </value>
		public string Name { get; private set; }

		internal Type PropertyType { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Column" /> class.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		public Column(string name)
		{
			this.Name = name;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Column" /> class.
		/// </summary>
		/// <param name="tableName">The name of the table.</param>
		/// <param name="name">The name of the column.</param>
		public Column(string tableName, string name)
		{
			this.Table = new Table(tableName);
			this.Name = name;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Column"/> class.
		/// </summary>
		/// <param name="table">The table.</param>
		/// <param name="name">The name.</param>
		public Column(Table table, string name)
		{
			this.Table = table;
			this.Name = name;
		}
		
		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Table != null)
			{
				b.Append(this.Table.ToString());
				b.Append(".");
			}
			b.Append(this.Name);
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public class Condition : ConditionExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Condition;
			}
		}

		public StatementPart Field { get; set; }

		public SqlOperator Operator { get; set; }

		public StatementPart Value { get; set; }

		internal Condition()
		{
		}

		// TODO: Make these static i.e. public static Condition Where(...) ??
		public Condition(string fieldName, SqlOperator op, object value)
		{
			this.Field = new Column(fieldName);
			this.Operator = op;
			AddValue(value);
		}

		public Condition(string tableName, string fieldName, SqlOperator op, object value)
		{
			this.Field = new Column(tableName, fieldName);
			this.Operator = op;
			AddValue(value);
		}

		public Condition(SourceExpression column, SqlOperator op, object value)
		{
			this.Field = column;
			this.Operator = op;
			AddValue(value);
		}

		public static Condition Where(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value);
		}

		public static Condition Where(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value);
		}

		public static Condition Or(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value) { Relationship = ConditionRelationship.Or };
		}

		public static Condition Or(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value) { Relationship = ConditionRelationship.Or };
		}

		public static Condition And(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value) { Relationship = ConditionRelationship.And };
		}

		public static Condition And(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value) { Relationship = ConditionRelationship.And };
		}

		private void AddValue(object value)
		{
			if (value == null)
			{
				this.Value = new ConstantPart(null);
				return;
			}

			//if (value is IEnumerable && !(value is string))
			//{
			//	foreach (object subval in (IEnumerable)value)
			//	{
			//		if (subval is StatementPart)
			//		{
			//			this.Value.Add((StatementPart)subval);
			//		}
			//		else
			//		{
			//			this.Value.Add(new ConstantPart(subval));
			//		}
			//	}
			//}
			//else
			//{
			if (value is StatementPart statementPartValue)
			{
				this.Value = statementPartValue;
			}
			else
			{
				this.Value = new ConstantPart(value);
			}
			//}
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Not)
			{
				b.Append("NOT ");
			}
			b.Append(this.Field.ToString());
			b.Append(" ");
			b.Append(this.Operator.ToString());
			b.Append(" ");
            if (this.Value == null)
            {
                b.Append("NULL");
            }
            else
            {
                b.Append(this.Value.ToString());
            }
			return b.ToString();
		}
	}

	public sealed class ConditionalCase : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionalCase;
			}
		}

		public StatementPart Test { get; set; }

		public StatementPart IfTrue { get; set; }

		public StatementPart IfFalse { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Test is Condition)
			{
				b.Append("(CASE WHEN ");
				b.Append(this.Test.ToString());
				b.Append(" THEN ");
				b.Append(this.IfTrue.ToString());
				var ifFalse = this.IfFalse;
				while (ifFalse is ConditionalCase ifFalseCase)
				{
					b.Append(" WHEN ");
					b.Append(ifFalseCase.Test.ToString());
					b.Append(" THEN ");
					b.Append(ifFalseCase.IfTrue.ToString());
					ifFalse = ifFalseCase.IfFalse;
				}
				b.Append(" ELSE ");
				b.Append(ifFalse.ToString());
				b.Append(")");
			}
			else
			{
				b.Append("(CASE ");
				b.Append(this.Test.ToString());
				b.Append(" WHEN True THEN ");
				b.Append(this.IfTrue.ToString());
				b.Append(" ELSE ");
				b.Append(this.IfFalse.ToString());
				b.Append(")");
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// A collection of conditions.
	/// </summary>
	public sealed class ConditionCollection : ConditionExpression, IEnumerable<ConditionExpression>
	{
		private readonly List<ConditionExpression> _conditions = new List<ConditionExpression>();

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionCollection;
			}
		}

		public int Count
		{
			get
			{
				return _conditions.Count;
			}
		}

		public ConditionExpression this[int index]
		{
			get
			{
				return _conditions[index];
			}
			set
			{
				_conditions[index] = value;
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConditionCollection"/> class.
		/// </summary>
		/// <param name="conditions">The conditions.</param>
		public ConditionCollection(params ConditionExpression[] conditions)
		{
			_conditions.AddRange(conditions);
		}

		public void Add(ConditionExpression item)
		{
			_conditions.Add(item);
		}

		public void Insert(int index, ConditionExpression item)
		{
			_conditions.Insert(index, item);
		}

		public void AddRange(IEnumerable<ConditionExpression> collection)
		{
			_conditions.AddRange(collection);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Not)
			{
				b.Append("Not ");
			}
			if (this.Count > 1)
			{
				b.Append("(");
			}
			for (var i = 0; i < this.Count; i++)
			{
				if (i > 0)
				{
					b.Append(" ");
					b.Append(this[i].Relationship.ToString());
					b.Append(" ");
				}
				b.Append(this[i].ToString());
			}
			if (this.Count > 1)
			{
				b.Append(")");
			}
			return b.ToString();
		}

		public IEnumerator<ConditionExpression> GetEnumerator()
		{
			return _conditions.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _conditions.GetEnumerator();
		}
	}

	/// <summary>
	/// An expression that can be used as a condition.
	/// </summary>
	public abstract class ConditionExpression : StatementPart
	{
		public ConditionRelationship Relationship { get; set; }

		public bool Not { get; set; }
	}

	// TODO: What even is this
	public sealed class ConditionPredicate : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionPredicate;
			}
		}

		public StatementPart Predicate { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("(CASE WHEN ");
			b.Append(this.Predicate.ToString());
			b.Append(" THEN True ELSE False)");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// The logical relationship between a set of conditions.
	/// </summary>
	public enum ConditionRelationship
	{
		/// <summary>
		/// The set of conditions should return true if all conditions are true.
		/// </summary>
		And,
		/// <summary>
		/// The set of conditions should return true if any conditions are true.
		/// </summary>
		Or,
	}

	/// <summary>
	/// A statement part containing a constant value.
	/// </summary>
	public sealed class ConstantPart : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConstantPart;
			}
		}

		/// <summary>
		/// Gets the constant value.
		/// </summary>
		/// <value>
		/// The constant value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConstantPart" /> class.
		/// </summary>
		/// <param name="value">The constant value.</param>
		public ConstantPart(object value)
		{
			this.Value = value;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Value == null)
			{
				b.Append("NULL");
			}
			else if (this.Value is string || this.Value is char || this.Value is DateTime)
			{
				b.Append("'");
				b.Append(this.Value.ToString());
				b.Append("'");
			}
			else if (this.Value is IEnumerable enumerable)
			{
				b.Append("{ ");
				var values = new List<string>();
				foreach (var o in enumerable)
				{
					if (o == null)
					{
						values.Add("NULL");
					}
                    else if (o is string || o is char || o is DateTime)
                    {
						values.Add("'" + o.ToString() + "'");
                    }
                    else
                    {
						values.Add(o.ToString());
					}
				}
				b.Append(string.Join(", ", values));
				b.Append(" }");
			}
			else
			{
				b.Append(this.Value.ToString());
			}
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// Converts an expression to the supplied type.
	/// </summary>
	public sealed class ConvertFunction : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConvertFunction;
			}
		}

		/// <summary>
		/// Gets or sets the expression to convert.
		/// </summary>
		/// <value>
		/// The expression to convert.
		/// </value>
		public SourceExpression Expression { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConvertFunction" /> class.
		/// </summary>
		internal ConvertFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConvertFunction" /> class.
		/// </summary>
		/// <param name="expression">The expression to convert.</param>
		public ConvertFunction(SourceExpression expression)
		{
			this.Expression = expression;
		}
	}

	public sealed class DateAddFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateAddFunction;
			}
		}

		public DatePart DatePart { get; set; }

		public StatementPart Argument { get; set; }

		public StatementPart Number { get; set; }

		internal DateAddFunction(DatePart datePart)
		{
			this.DatePart = datePart;
		}

		public override string ToString()
		{
			return $"DATEADD({this.DatePart}, {this.Argument}, {this.Number})";
		}
	}

	public sealed class DateDifferenceFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateDifferenceFunction;
			}
		}

		public StatementPart Date1 { get; set; }

		public StatementPart Date2 { get; set; }
	}

	public sealed class DateNewFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateNewFunction;
			}
		}

		public StatementPart Year { get; set; }

		public StatementPart Month { get; set; }

		public StatementPart Day { get; set; }

		public StatementPart Hour { get; set; }

		public StatementPart Minute { get; set; }

		public StatementPart Second { get; set; }

		public DateNewFunction()
		{
		}

		public override string ToString()
		{
			if (this.Hour != null || this.Minute != null || this.Second != null)
			{
				return "DATENEW(" + this.Year.ToString() + ", " + this.Month.ToString() + ", " + this.Day.ToString() + ", " + this.Hour.ToString() + ", " + this.Minute.ToString() + ", " + this.Second.ToString() + ")";
			}
			else
			{
				return "DATENEW(" + this.Year.ToString() + ", " + this.Month.ToString() + ", " + this.Day.ToString() + ")";
			}
		}
	}

	/// <summary>
	/// A date part.
	/// </summary>
	public enum DatePart
	{
		/// <summary>
		/// The millisecond component of the date's time.
		/// </summary>
		Millisecond,
		/// <summary>
		/// The second component of the date's time.
		/// </summary>
		Second,
		/// <summary>
		/// The minute component of the date's time.
		/// </summary>
		Minute,
		/// <summary>
		/// The hour component of the date's time.
		/// </summary>
		Hour,
		/// <summary>
		/// The day component of the date.
		/// </summary>
		Day,
		/// <summary>
		/// The day of the week component of the date.
		/// </summary>
		DayOfWeek,
		/// <summary>
		/// The day of the year component of the date.
		/// </summary>
		DayOfYear,
		/// <summary>
		/// The month component of the date.
		/// </summary>
		Month,
		/// <summary>
		/// The year component of the date.
		/// </summary>
		Year,
		/// <summary>
		/// The date component of the date.
		/// </summary>
		Date,
	}

	public sealed class DatePartFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DatePartFunction;
			}
		}

		public DatePart DatePart { get; set; }

		public StatementPart Argument { get; set; }

		internal DatePartFunction(DatePart datePart)
		{
			this.DatePart = datePart;
		}

		public override string ToString()
		{
			return $"DATEPART({this.DatePart}, {this.Argument})";
		}
	}


	public sealed class Exists : ConditionExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Exists;
			}
		}

		public SelectStatement Select { get; set; }

		public override string ToString()
		{
			if (this.Not)
			{
				return "NOT EXISTS " + this.Select.ToString();
			}
			else
			{
				return "EXISTS " + this.Select.ToString();
			}
		}
	}

	internal static class ExpressionExtensions
	{
		public static Expression Equal(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.Equal(expression1, expression2);
		}

		public static Expression NotEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.NotEqual(expression1, expression2);
		}

		public static Expression GreaterThan(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.GreaterThan(expression1, expression2);
		}

		public static Expression GreaterThanOrEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.GreaterThanOrEqual(expression1, expression2);
		}

		public static Expression LessThan(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.LessThan(expression1, expression2);
		}

		public static Expression LessThanOrEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.LessThanOrEqual(expression1, expression2);
		}

		public static Expression And(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.And(expression1, expression2);
		}

		public static Expression AndAlso(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.AndAlso(expression1, expression2);
		}

		public static Expression Or(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.Or(expression1, expression2);
		}

		public static Expression OrElse(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.OrElse(expression1, expression2);
		}

		public static Expression Binary(this Expression expression1, ExpressionType op, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.MakeBinary(op, expression1, expression2);
		}

		private static void ConvertExpressions(ref Expression expression1, ref Expression expression2)
		{
			if (expression1.Type != expression2.Type)
			{
				var isNullable1 = TypeHelper.IsNullableType(expression1.Type);
				var isNullable2 = TypeHelper.IsNullableType(expression2.Type);
				if (isNullable1 || isNullable2)
				{
					if (TypeHelper.GetNonNullableType(expression1.Type) == TypeHelper.GetNonNullableType(expression2.Type))
					{
						if (!isNullable1)
						{
							expression1 = Expression.Convert(expression1, expression2.Type);
						}
						else if (!isNullable2)
						{
							expression2 = Expression.Convert(expression2, expression1.Type);
						}
					}
				}
			}
		}
	}

	// TODO: I can't remember what the difference is between a Field and a SourceExpression
	//			Figure it out and document it, or combine the two classes
	public abstract class Field : SourceExpression
	{
	}

	/// <summary>
	/// A collection of fields.
	/// </summary>
	public sealed class FieldCollection : SourceExpression, IEnumerable<SourceExpression>
	{
		private readonly List<SourceExpression> _fields = new List<SourceExpression>();

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.FieldCollection;
			}
		}
		
		public int Count
		{
			get
			{
				return _fields.Count;
			}
		}

		public SourceExpression this[int index]
		{
			get
			{
				return _fields[index];
			}
			set
			{
				_fields[index] = value;
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldCollection"/> class.
		/// </summary>
		/// <param name="fields">The fields.</param>
		public FieldCollection(params Field[] fields)
		{
			_fields.AddRange(fields);
		}

		public void Add(SourceExpression item)
		{
			_fields.Add(item);
		}

		public void Insert(int index, SourceExpression item)
		{
			_fields.Insert(index, item);
		}

		public void AddRange(IEnumerable<SourceExpression> collection)
		{
			_fields.AddRange(collection);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Count > 1)
			{
				b.Append("(");
			}
			for (var i = 0; i < this.Count; i++)
			{
				if (i > 0)
				{
					b.Append(", ");
				}
				b.Append(this[i].ToString());
			}
			if (this.Count > 1)
			{
				b.Append(")");
			}
			return b.ToString();
		}

		public IEnumerator<SourceExpression> GetEnumerator()
		{
			return _fields.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _fields.GetEnumerator();
		}
	}

	public abstract class GenericStatement : Statement
	{
		public abstract Statement CreateStatement(DatabaseMapper mapper);
	}

	public sealed class Join : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Join;
			}
		}

		public JoinType JoinType { get; internal set; }
		
		public StatementPart Table { get; internal set; }

		public ConditionCollection Conditions { get; } = new ConditionCollection();

		internal Join()
		{
		}

		public Join(JoinType joinType, StatementPart right, ConditionExpression condition)
		{
			this.JoinType = joinType;
			this.Table = right;
			this.Conditions.Add(condition);
		}

		public Join(string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			this.JoinType = JoinType.Inner;
			// TODO: Fix this pug fugly syntax
			// TODO: Change field => column in all the SQL stuff?  Column if it's a column, field if it's a statement part
			//this.Left = new Table(leftTableName);
			this.Table = new Table(tableName);
			this.Conditions.Add(new Condition(leftTableName, leftColumnName, SqlOperator.Equals, new Column(rightTableName, rightColumnName)));
		}

		public Join(JoinType joinType, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			this.JoinType = joinType;
			// TODO: Fix this pug fugly syntax
			// TODO: Change field => column in all the SQL stuff?  Column if it's a column, field if it's a statement part
			//this.Left = new Table(leftTableName);
			this.Table = new Table(tableName);
			this.Conditions.Add(new Condition(leftTableName, leftColumnName, SqlOperator.Equals, new Column(rightTableName, rightColumnName)));
		}

		public Join(Table table, SourceExpression leftColumn, SourceExpression rightColumn)
		{
			this.JoinType = JoinType.Inner;
			this.Table = table;
			this.Conditions.Add(new Condition(leftColumn, SqlOperator.Equals, rightColumn));
		}

		public Join(JoinType joinType, Table table, SourceExpression leftColumn, SourceExpression rightColumn)
		{
			this.JoinType = joinType;
			this.Table = table;
			this.Conditions.Add(new Condition(leftColumn, SqlOperator.Equals, rightColumn));
		}

		public Join(Table table, ConditionCollection conditions)
		{
			this.JoinType = JoinType.Inner;
			this.Table = table;
			this.Conditions.AddRange(conditions);
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.JoinType.ToString());
			b.Append(" JOIN ");
			b.Append(this.Table.ToString());
			if (this.Conditions.Count > 0)
			{
				b.Append(" ON ");
				b.Append(this.Conditions.ToString());
			}
			return b.ToString();
		}
	}

	public enum JoinType
	{
		Inner,
		Left,
		Right,
		Cross,
		CrossApply,
		OuterApply
	}

	public sealed class LiteralPart : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.LiteralPart;
			}
		}

		public string Value { get; private set; }

		public LiteralPart(string value)
		{
			this.Value = value;
		}
	}

	public sealed class NumberAbsoluteFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberAbsoluteFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "ABSOLUTE(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberCeilingFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberCeilingFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "CEILING(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberExponentialFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberExponentialFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "EXPONENTIAL(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberFloorFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberFloorFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "FLOOR(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberLog10Function : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberLog10Function;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LOG10(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberLogFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberLogFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LOG(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberNegateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberNegateFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "NEGATE(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberPowerFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberPowerFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Power { get; set; }

		public override string ToString()
		{
			return "POWER(" + this.Argument.ToString() + ", " + this.Power.ToString() + ")";
		}
	}

	public sealed class NumberRootFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberRootFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Root { get; set; }

		public override string ToString()
		{
			return "ROOT(" + this.Argument.ToString() + ", " + this.Root.ToString() + ")";
		}
	}

	public sealed class NumberRoundFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberRoundFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Precision { get; set; }

		public override string ToString()
		{
			return "ROUND(" + this.Argument.ToString() + ", " + this.Precision + ")";
		}
	}

	public sealed class NumberSignFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberSignFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "SIGN(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberTrigFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberTrigFunction;
			}
		}

		public TrigFunction Function { get; set; }

		public StatementPart Argument { get; set; }

		// For Atan2
		public StatementPart Argument2 { get; set; }

		public override string ToString()
		{
			return this.Function.ToString() + "(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberTruncateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberTruncateFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TRUNCATE(" + this.Argument.ToString() + ")";
		}
	}

	/// <summary>
	/// An expression that is used to order a select statement.
	/// </summary>
	public sealed class OrderByExpression : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.OrderByField;
			}
		}

		/// <summary>
		/// Gets the expression that is ordered by.
		/// </summary>
		/// <value>
		/// The expression.
		/// </value>
		public SourceExpression Expression { get; internal set; }

		/// <summary>
		/// Gets the direction of ordering.
		/// </summary>
		/// <value>
		/// The direction.
		/// </value>
		public OrderDirection Direction { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		internal OrderByExpression()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="expression">The expression that is ordered by.</param>
		public OrderByExpression(SourceExpression expression)
		{
			this.Expression = expression;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="columnName">The name of the column to order by.</param>
		public OrderByExpression(string columnName)
		{
			this.Expression = new Column(columnName);
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="expression">The expression that is ordered by.</param>
		/// <param name="direction">The direction of ordering.</param>
		public OrderByExpression(SourceExpression expression, OrderDirection direction)
		{
			this.Expression = expression;
			this.Direction = direction;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="columnName">The name of the column to order by.</param>
		/// <param name="direction">The direction of ordering.</param>
		public OrderByExpression(string columnName, OrderDirection direction)
		{
			this.Expression = new Column(columnName);
			this.Direction = direction;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			if (this.Direction == OrderDirection.Ascending)
			{
				return this.Expression.ToString();
			}
			else
			{
				return this.Expression.ToString() + " " + this.Direction.ToString();
			}
		}
	}

	/// <summary>
	/// The direction in which an expression is ordered.
	/// </summary>
	public enum OrderDirection
	{
		/// <summary>
		/// The expression is ordered from lowest to highest.
		/// </summary>
		Ascending,
		/// <summary>
		/// The expression is ordered from highest to lowest.
		/// </summary>
		Descending,
	}

	/// <summary>
	/// A parameter for passing to a stored procedure or function.
	/// </summary>
	public class Parameter
	{
		/// <summary>
		/// Gets the name of the parameter.
		/// </summary>
		/// <value>
		/// The name.
		/// </value>
		public string Name { get; private set; }

		/// <summary>
		/// Gets the value of the parameter.
		/// </summary>
		/// <value>
		/// The value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Gets the type of the parameter.
		/// </summary>
		/// <value>
		/// The type of the parameter.
		/// </value>
		public Type ParameterType { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Parameter"/> class for use in a query.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="value">The value.</param>
		public Parameter(string name, object value)
		{
			this.Name = name;
			this.Value = value;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Parameter" /> class for use when defining a procedure or function.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="parameterType">Type of the parameter.</param>
		public Parameter(string name, Type parameterType)
		{
			this.Name = name;
			this.ParameterType = parameterType;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name;
		}
	}

	public sealed class RowNumber : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.RowNumber;
			}
		}

		public List<OrderByExpression> OrderByFields { get; } = new List<OrderByExpression>();

		public RowNumber(params OrderByExpression[] orderByFields)
		{
			this.OrderByFields.AddRange(orderByFields);
		}

		public override string ToString()
		{
			return "ROWNUMBER";
		}
	}

	/// <summary>
	/// A field containing a select statement that returns a single value.
	/// </summary>
	public sealed class ScalarField : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		/// <exception cref="NotImplementedException"></exception>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ScalarField;
			}
		}

		/// <summary>
		/// Gets the select statement.
		/// </summary>
		/// <value>
		/// The select statement.
		/// </value>
		public SelectStatement Select { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ScalarField"/> class.
		/// </summary>
		internal ScalarField()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ScalarField"/> class.
		/// </summary>
		/// <param name="select">The select statement.</param>
		public ScalarField(SelectStatement select)
		{
			this.Select = select;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.Select.ToString());
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append("s");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public class SelectExpression : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.SelectExpression;
			}
		}

		public SelectStatement Select { get; set; }

		public SelectExpression(SelectStatement select, string alias = null)
		{
			this.Select = select;
			this.Alias = alias;
		}
	}

	public sealed class SetValue
	{
		public Column Column { get; set; }

		public StatementPart Value { get; set; }

		public SetValue()
		{
		}

		public SetValue(string columnName, object value)
			: this(new Column(columnName), value)
		{
		}

		public SetValue(Column column, object value)
		{
			this.Column = column;
			if (value is StatementPart statementPartValue)
			{
				this.Value = statementPartValue;
			}
			else
			{
				this.Value = new ConstantPart(value);
			}
		}
	}

	/// <summary>
	/// An expression that can be used in the field list of a select statement.
	/// </summary>
	public abstract class SourceExpression : StatementPart
	{
		public string Alias { get; set; }
	}

	public enum SqlOperator
	{
		Equals,
		NotEquals,
		IsLessThan,
		IsLessThanOrEqualTo,
		IsGreaterThan,
		IsGreaterThanOrEqualTo,
		IsIn,
		Contains,
		StartsWith,
		EndsWith
	}

	public abstract class Statement : StatementPart
	{
		public Command Build()
		{
			return Build(new DatabaseMapper(), new SqlCommandBuilder());
		}

		public Command Build(DatabaseMapper mapper)
		{
			return Build(mapper, new SqlCommandBuilder());
		}

		public Command Build(ICommandBuilder builder)
		{
			return Build(new DatabaseMapper(), builder);
		}

		public Command Build(DatabaseMapper mapper, ICommandBuilder builder)
		{
			builder.VisitStatement(this, mapper);
			return new Command(this, builder.CommandText.ToString(), builder.ParameterValues.ToArray());
		}
	}

	/// <summary>
	/// The basic building blocks of SQL statements.
	/// </summary>
	public abstract class StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public abstract StatementPartType PartType { get; }
	}

	public enum StatementPartType
	{
		Select,
		GenericSelect,
		//Insert,
		//GenericInsert,
		//Update,
		//GenericUpdate,
		//Delete,
		//GenericDelete,
		Table,
		Column,
		Join,
		Aggregate,
		Exists,
		// TODO: Rename that!
		ConstantPart,
		OrderByField,
		BinaryOperation,
		UnaryOperation,
		ConditionalCase,
		CoalesceFunction,
		DateDifferenceFunction,
		DateNewFunction,
		DatePartFunction,
		DateAddFunction,
		NumberAbsoluteFunction,
		NumberCeilingFunction,
		NumberFloorFunction,
		NumberNegateFunction,
		NumberRoundFunction,
		NumberTruncateFunction,
		NumberSignFunction,
		NumberPowerFunction,
		NumberRootFunction,
		NumberExponentialFunction,
		NumberLogFunction,
		NumberLog10Function,
		NumberTrigFunction,
		StringIndexFunction,
		StringCompareFunction,
		StringConcatenateFunction,
		StringLengthFunction,
		StringRemoveFunction,
		StringReplaceFunction,
		StringTrimFunction,
		StringToLowerFunction,
		StringToUpperFunction,
		SubstringFunction,
		ConditionPredicate,
		Parameter,
		ConvertFunction,
		ConditionExpression,
		Condition,
		ConditionCollection,
		RowNumber,
		LiteralPart,
		ScalarField,
		FieldCollection,
		SelectExpression,
		UserDefinedFunction
	}

	public sealed class StringCompareFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringCompareFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Other { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("COMPARE(");
			b.Append(this.Argument.ToString());
			b.Append(", ");
			b.Append(this.Other.ToString());
			b.Append(")");
			return b.ToString();
		}
	}

	public sealed class StringConcatenateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringConcatenateFunction;
			}
		}

		public List<StatementPart> Arguments { get; } = new List<StatementPart>();

		public override string ToString()
		{
			return "CONCAT(" + string.Join(", ", this.Arguments.Select(a => a.ToString())) + ")";
		}
	}

	public sealed class StringIndexFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringIndexFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StringToFind { get; set; }

		public StatementPart StartIndex { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("INDEXOF(");
			b.Append(this.Argument.ToString());
			b.Append(", ");
			b.Append(this.StringToFind.ToString());
			if (this.StartIndex != null)
			{
				b.Append(", ");
				b.Append(this.StartIndex.ToString());
			}
			b.Append(")");
			return b.ToString();
		}
	}

	public sealed class StringLengthFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringLengthFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LENGTH(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringRemoveFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringRemoveFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StartIndex { get; set; }

		public StatementPart Length { get; set; }

		public override string ToString()
		{
			return "REMOVE(" + this.Argument.ToString() + ", " + this.StartIndex.ToString() + ", " + this.Length.ToString() + ")";
		}
	}

	public sealed class StringReplaceFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringReplaceFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart OldValue { get; set; }

		public StatementPart NewValue { get; set; }

		public override string ToString()
		{
			return "REPLACE(" + this.Argument.ToString() + ", " + this.OldValue.ToString() + ", " + this.NewValue.ToString() + ")";
		}
	}

	public sealed class StringToLowerFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringToLowerFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TOLOWER(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringToUpperFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringToUpperFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TOUPPER(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringTrimFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringTrimFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TRIM(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class SubstringFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.SubstringFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StartIndex { get; set; }

		public StatementPart Length { get; set; }

		public override string ToString()
		{
			return "SUBSTRING(" + this.Argument.ToString() + ", " + this.StartIndex.ToString() + ", " + this.Length.ToString() + ")";
		}
	}

	/// <summary>
	/// A table in the database.
	/// </summary>
	public sealed class Table : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Table;
			}
		}

		/// <summary>
		/// Gets the name of the schema.
		/// </summary>
		/// <value>
		/// The name of the schema.
		/// </value>
		public string Schema { get; internal set; }

		/// <summary>
		/// Gets the name of the table.
		/// </summary>
		/// <value>
		/// The name of the table.
		/// </value>
		public string Name { get; internal set; }

		/// <summary>
		/// Gets the alias to use for the table.
		/// </summary>
		/// <value>
		/// The alias.
		/// </value>
		public string Alias { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Table" /> class.
		/// </summary>
		internal Table()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Table" /> class.
		/// </summary>
		/// <param name="name">The name of the table.</param>
		/// <param name="alias">The alias to use for the table.</param>
		/// <param name="schema">The schema to use for the table.</param>
		public Table(string name, string alias = null, string schema = null)
		{
			this.Name = name;
			this.Alias = alias;
			this.Schema = schema;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name + (!string.IsNullOrEmpty(this.Alias) ? " AS " + this.Alias : "");
		}
	}

	public sealed class Table<T>
	{
		public Type Type { get; internal set; }

		public string Alias { get; internal set; }

		public Table(Type type)
		{
			this.Type = type;
		}

		public Table(Type type, string alias)
		{
			this.Type = type;
			this.Alias = alias;
		}
	}

	public enum TrigFunction
	{
		Sin,
		Cos,
		Tan,
		Asin,
		Acos,
		Atan,
		Atan2
	}

	/// <summary>
	/// An operation with a single operator e.g. negative 1.
	/// </summary>
	public sealed class UnaryOperation : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.UnaryOperation;
			}
		}

		/// <summary>
		/// Gets or sets the operator.
		/// </summary>
		/// <value>
		/// The operator.
		/// </value>
		public UnaryOperator Operator { get; set; }

		/// <summary>
		/// Gets or sets the expression.
		/// </summary>
		/// <value>
		/// The expression.
		/// </value>
		public StatementPart Expression { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="UnaryOperation" /> class.
		/// </summary>
		internal UnaryOperation()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="UnaryOperation" /> class.
		/// </summary>
		/// <param name="op">The operator.</param>
		/// <param name="expression">The expression.</param>
		public UnaryOperation(UnaryOperator op, StatementPart expression)
		{
			this.Operator = op;
			this.Expression = expression;
		}

		public override string ToString()
		{
			return this.Operator.ToString() + " " + this.Expression.ToString();
		}
	}

	/// <summary>
	/// An operator that is performed on a single expression.
	/// </summary>
	public enum UnaryOperator
	{
		/// <summary>
		/// Makes the expression logically opposite.
		/// </summary>
		Not,
		/// <summary>
		/// Negates the expression.
		/// </summary>
		Negate,
	}

	/// <summary>
	/// A user-defined function in the database.
	/// </summary>
	public sealed class UserDefinedFunction : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.UserDefinedFunction;
			}
		}

		/// <summary>
		/// Gets the name of the schema.
		/// </summary>
		/// <value>
		/// The name of the schema.
		/// </value>
		public string Schema { get; internal set; }

		/// <summary>
		/// Gets the name of the function.
		/// </summary>
		/// <value>
		/// The name of the function.
		/// </value>
		public string Name { get; internal set; }

		/// <summary>
		/// Gets the alias to use for the function.
		/// </summary>
		/// <value>
		/// The alias.
		/// </value>
		public string Alias { get; internal set; }

		/// <summary>
		/// Gets the paths of related items and collections to include when loading data from this function.
		/// </summary>
		/// <value>
		/// The include paths.
		/// </value>
		public List<Parameter> Parameters { get; } = new List<Parameter>();

		/// <summary>
		/// Initializes a new instance of the <see cref="UserDefinedFunction" /> class.
		/// </summary>
		internal UserDefinedFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="UserDefinedFunction" /> class.
		/// </summary>
		/// <param name="name">The name of the function.</param>
		/// <param name="alias">The alias to use for the function.</param>
		public UserDefinedFunction(string name, string alias = null)
		{
			this.Name = name;
			this.Alias = alias;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name + (!string.IsNullOrEmpty(this.Alias) ? " AS " + this.Alias : "");
		}
	}

	public sealed class SelectStatement : Statement
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Select;
			}
		}

		public StatementPart Source { get; internal set; }

		public List<Join> SourceJoins { get; } = new List<Join>();

		public List<string> IncludePaths { get; } = new List<string>();

		public List<SourceExpression> SourceFields { get; } = new List<SourceExpression>();

		public List<Table> SourceFieldsFrom { get; } = new List<Table>();

		public bool IsAny { get; set; }

		public bool IsAll { get; set; }

		public bool IsContains { get; set; }

		public StatementPart ContainsItem { get; set; }

		public bool IsDistinct { get; set; }

		public int StartIndex { get; set; }

		public int Limit { get; set; }

		public ConditionCollection Conditions { get; } = new ConditionCollection();

		public List<OrderByExpression> OrderByFields { get; } = new List<OrderByExpression>();

		public List<Column> GroupByFields { get; } = new List<Column>();

		public List<SelectStatement> UnionStatements { get; } = new List<SelectStatement>();

		public string Alias { get; set; }

		public bool IsAggregate { get; set; }

		internal SelectStatement()
		{
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("(");
			b.Append("SELECT ");
			if (this.IsAny)
			{
				b.Append("ANY ");
			}
			if (this.IsAll)
			{
				b.Append("ALL ");
			}
			if (this.IsContains)
			{
				b.Append("CONTAINS ");
				b.Append(this.ContainsItem);
				b.Append(" IN ");
			}
			if (this.IsDistinct)
			{
				b.Append("DISTINCT ");
			}
			if (this.Limit == 1)
			{
				b.Append("(Row ");
				b.Append(this.StartIndex);
				b.Append(") ");
			}
			else if (this.StartIndex != 0 || this.Limit != 0)
			{
				b.Append("(Rows ");
				b.Append(this.StartIndex);
				if (this.Limit == 0)
				{
					b.Append("+");
				}
				else
				{
					b.Append("-");
					b.Append(this.StartIndex + this.Limit);
				}
				b.Append(") ");
			}
			if (this.SourceFields.Count > 0)
			{
				b.Append(string.Join(", ", Array.ConvertAll(this.SourceFields.ToArray(), f => f.ToString())));
			}
			else
			{
				b.Append("* ");
			}
			b.AppendLine(" ");
			if (this.Source != null)
			{
				b.Append("FROM ");
				b.Append(this.Source.ToString());
				b.AppendLine(" ");
			}
			// TODO: Do these ever get used?
			if (this.SourceJoins.Count > 0)
			{
				b.Append("JOIN ");
				b.Append(string.Join(" AND ", Array.ConvertAll(this.SourceJoins.ToArray(), j => j.ToString())));
				b.AppendLine(" ");
			}
			if (this.Conditions.Count > 0)
			{
				b.Append("WHERE ");
				b.Append(this.Conditions.ToString());
				b.AppendLine(" ");
			}
			if (this.GroupByFields.Count > 0)
			{
				b.Append("GROUP BY ");
				b.Append(string.Join(", ", Array.ConvertAll(this.GroupByFields.ToArray(), f => f.ToString())));
			}
			if (this.OrderByFields.Count > 0)
			{
				b.Append("ORDER BY ");
				b.Append(string.Join(", ", Array.ConvertAll(this.OrderByFields.ToArray(), f => f.ToString())));
			}
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public sealed class SelectStatement<T> : GenericStatement
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.GenericSelect;
			}
		}

		public Table<T> Source { get; internal set; }

		public List<PropertyInfo> SourceFields { get; } = new List<PropertyInfo>();

		public List<FieldAggregate> AggregateFields { get; } = new List<FieldAggregate>();

		public bool IsAny { get; set; }

		public bool IsAll { get; set; }

		public bool IsDistinct { get; set; }

		public int StartIndex { get; set; }

		public int Limit { get; set; }

		public Expression<Func<T, bool>> Conditions { get; internal set; }

		public List<FieldOrder> OrderByFields { get; internal set; } = new List<FieldOrder>();

		public List<PropertyInfo> GroupByFields { get; internal set; } = new List<PropertyInfo>();

		internal SelectStatement(string alias = null)
		{
			this.Source = new Table<T>(typeof(T), alias);
		}

		public override Statement CreateStatement(DatabaseMapper mapper)
		{
			var select = new SelectStatement();
			select.Source = new Table(mapper.GetTableName(this.Source.Type), this.Source.Alias);
			select.SourceFields.AddRange(this.SourceFields.Select(s => PropertyToSourceField(s, mapper)));
			select.SourceFields.AddRange(this.AggregateFields.Select(s => PropertyToAggregate(s, mapper)));
			select.IsAny = this.IsAny;
			select.IsAll = this.IsAll;
			select.IsDistinct = this.IsDistinct;
			select.StartIndex = this.StartIndex;
			select.Limit = this.Limit;
			if (this.Conditions != null)
			{
				// TODO: Need to handle columns from multiple tables...
				var aliasTables = !string.IsNullOrEmpty(this.Source.Alias);
				foreach (var condition in StatementCreator.VisitStatementConditions(this.Conditions, mapper, aliasTables))
				{
					select.Conditions.Add(condition);
				}
			}
			select.OrderByFields.AddRange(this.OrderByFields.Select(s => PropertyToOrderBy(s, mapper)));
			select.GroupByFields.AddRange(this.GroupByFields.Select(s => PropertyToGroupBy(s, mapper)));
			return select;
		}

		private SourceExpression PropertyToSourceField(PropertyInfo prop, DatabaseMapper mapper)
		{
			if (prop != null)
			{
				return new Column(TableNameOrAlias(mapper, prop.DeclaringType), mapper.GetColumnName(prop));
			}
			else
			{
				return new ConstantPart(null);
			}
		}

		private SourceExpression PropertyToAggregate(FieldAggregate field, DatabaseMapper mapper)
		{
			return new Aggregate(
				field.Aggregate,
				new Column(
					field.Field != null ? TableNameOrAlias(mapper, field.Field.DeclaringType) : "",
					field.Field != null ? mapper.GetColumnName(field.Field) : "*")
				);
		}

		private OrderByExpression PropertyToOrderBy(FieldOrder field, DatabaseMapper mapper)
		{
			return new OrderByExpression(
				new Column(
					TableNameOrAlias(mapper, field.Field.DeclaringType),
					mapper.GetColumnName(field.Field)), field.Direction);
		}

		private Column PropertyToGroupBy(PropertyInfo prop, DatabaseMapper mapper)
		{
			return new Column(
					 TableNameOrAlias(mapper, prop.DeclaringType),
					 mapper.GetColumnName(prop));
		}

		private string TableNameOrAlias(DatabaseMapper mapper, Type t)
		{
			if (t == this.Source.Type && !string.IsNullOrEmpty(this.Source.Alias))
			{
				return this.Source.Alias;
			}
			else
			{
				return mapper.GetTableName(t);
			}
		}
	}

	/// <summary>
	/// Contains command text and parameters for running a statement against a database.
	/// </summary>
	public class Command
	{
		/// <summary>
		/// Gets the statement that this command was built from.
		/// </summary>
		/// <value>
		/// The statement.
		/// </value>
		public Statement Statement { get; }

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public string CommandText { get; }

		/// <summary>
		/// Gets the parameters.
		/// </summary>
		/// <value>
		/// The parameters.
		/// </value>
		public IList<object> Parameters { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Command"/> class.
		/// </summary>
		/// <param name="statement">The statement that this command was built from.</param>
		/// <param name="commandText">The command text.</param>
		/// <param name="parameters">The parameters.</param>
		public Command(Statement statement, string commandText, object[] parameters)
		{
			this.Statement = statement;
			this.CommandText = commandText;
			this.Parameters = parameters;
		}
	}

	/// <summary>
	/// Maps .NET objects to database objects.
	/// </summary>
	public class DatabaseMapper
	{
		/// <summary>
		/// Gets the namespace in which entity classes are located.
		/// </summary>
		/// <value>
		/// The entity namespace.
		/// </value>
		public string EntityNamespace { get; set; } = "$";

		/// <summary>
		/// Gets the name of the schema for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetSchemaName(Type type)
		{
			return string.Empty;
		}

		/// <summary>
		/// Gets the name of the table for the supplied type.
		/// </summary>
		/// <remarks>
		/// For a Book item, this would return "Book" by default but might be overridden to return "Books" or something different.
		/// </remarks>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetTableName(Type type)
		{
			return type.Name;
		}

		/// <summary>
		/// Gets the name of the procedure for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetProcedureName(Type type)
		{
			return type.Name.Replace("Procedure", "");
		}

		/// <summary>
		/// Gets the name of the function for the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetFunctionName(Type type)
		{
			return type.Name.Replace("Function", "");
		}

		/// <summary>
		/// Gets the name of the column for the supplied property.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetColumnName(PropertyInfo property)
		{
			return property.Name;
		}

		/// <summary>
		/// Gets the name of the primary key column.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual string GetPrimaryKeyColumnName(Type type)
		{
			return "Id";
		}

		/// <summary>
		/// Determines whether the supplied property contains a related entity item.
		/// </summary>
		/// <param name="property">The property.</param>
		/// <returns>
		///   <c>true</c> if the supplied property contains a related entity item; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsRelatedItem(PropertyInfo property)
		{
			return ShouldMapType(property.PropertyType);
		}

		/// <summary>
		/// Gets the name of the foreign key column for the supplied property.
		/// </summary>
		/// <remarks>
		/// For a Book.Author property, this would return "AuthorID" by default.
		/// </remarks>
		/// <param name="property">The property.</param>
		/// <returns></returns>
		public virtual string GetForeignKeyColumnName(PropertyInfo property)
		{
			return property.Name + "Id";
		}

		/// <summary>
		/// Determines whether the supplied type is a stored procedure.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a stored procedure; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsProcedure(Type type)
		{
			return type.Name.EndsWith("Procedure");
		}

		/// <summary>
		/// Determines whether the supplied type is a user-defined function.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns>
		///   <c>true</c> if the supplied type is a user-defined function; otherwise, <c>false</c>.
		/// </returns>
		public virtual bool IsFunction(Type type)
		{
			return type.Name.EndsWith("Function");
		}

		/// <summary>
		/// Determines whether the class with the supplied type should be mapped to the database.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public virtual bool ShouldMapType(Type type)
		{
			return (type.Namespace == this.EntityNamespace);
		}
	}

	/// <summary>
	/// Represents a field and aggregate (count, sum, etc) that is used with a select statement.
	/// </summary>
	public class FieldAggregate
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the aggregate (count, sum, etc).
		/// </summary>
		/// <value>
		/// The aggregate.
		/// </value>
		public AggregateType Aggregate { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldAggregate"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="aggregate">The aggregate (count, sum, etc).</param>
		public FieldAggregate(PropertyInfo field, AggregateType aggregate)
		{
			this.Field = field;
			this.Aggregate = aggregate;
		}
	}

	/// <summary>
	/// Represents a field and direction that is used for ordering a statement.
	/// </summary>
	public class FieldOrder
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the order direction (ascending or descending).
		/// </summary>
		/// <value>
		/// The direction.
		/// </value>
		public OrderDirection Direction { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldOrder"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="direction">The order direction (ascending or descending).</param>
		public FieldOrder(PropertyInfo field, OrderDirection direction = OrderDirection.Ascending)
		{
			this.Field = field;
			this.Direction = direction;
		}
	}

	/// <summary>
	/// Represents a field and the value to set it to.
	/// </summary>
	public class FieldValue
	{
		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public PropertyInfo Field { get; private set; }

		/// <summary>
		/// Gets or sets the value to set the field to.
		/// </summary>
		/// <value>
		/// The value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldValue"/> class.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <param name="value">The value to set the field to.</param>
		public FieldValue(PropertyInfo field, object value)
		{
			this.Field = field;
			this.Value = value;
		}
	}

	/// <summary>
	/// An interface for building command text and parameters from a statement.
	/// </summary>
	public interface ICommandBuilder
	{
		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		StringBuilder CommandText { get; }

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		List<object> ParameterValues { get; }

		/// <summary>
		/// Visits the statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		void VisitStatement(Statement statement, DatabaseMapper mapper);
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement with the name of the table that records should be selected from.
		/// </summary>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="alias">The alias.</param>
		/// <param name="schema">The schema.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(string tableName, string alias = null, string schema = null)
		{
			return Select.From(new Table(tableName, alias, schema));
		}

		/// <summary>
		/// Creates a select statement with the table that records should be selected from.
		/// </summary>
		/// <param name="table">The table.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Table table)
		{
			return new SelectStatement() { Source = table };
		}

		/// <summary>
		/// Creates a select statement from a join.
		/// </summary>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(Join join)
		{
			return new SelectStatement() { Source = join };
		}

		/// <summary>
		/// Creates a select statement from a statement part.
		/// </summary>
		/// <param name="part">The part.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement From(StatementPart part)
		{
			return new SelectStatement() { Source = part };
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="join">The join.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Join join)
		{
			select.SourceJoins.Add(join);
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="tableName">Name of the table.</param>
		/// <param name="leftTableName">Name of the left table.</param>
		/// <param name="leftColumnName">Name of the left column.</param>
		/// <param name="rightTableName">Name of the right table.</param>
		/// <param name="rightColumnName">Name of the right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			select.SourceJoins.Add(new Join(joinType, tableName, leftTableName, leftColumnName, rightTableName, rightColumnName));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a join to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="joinType">Type of the join.</param>
		/// <param name="table">The table.</param>
		/// <param name="leftColumn">The left column.</param>
		/// <param name="rightColumn">The right column.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Join(this SelectStatement select, JoinType joinType, Table table, Column leftColumn, Column rightColumn)
		{
			select.SourceJoins.Add(new Join(joinType, table, leftColumn, rightColumn));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params string[] columnNames)
		{
			select.SourceFields.AddRange(columnNames.Select(cn => new Column(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Columns(this SelectStatement select, params SourceExpression[] columns)
		{
			select.SourceFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tableNames">The table names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params string[] tableNames)
		{
			select.SourceFieldsFrom.AddRange(tableNames.Select(tn => new Table(tn)));
			return select;
		}

		/// <summary>
		/// Adds a list of tables to select columns from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="tables">The tables.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement ColumnsFrom(this SelectStatement select, params Table[] tables)
		{
			select.SourceFieldsFrom.AddRange(tables);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Count, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to COUNT to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Count(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Count, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Sum, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to SUM to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Sum(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Sum, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Sum, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Min, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MIN to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Min(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Min, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Min, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Max, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to MAX to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Max(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Max, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Max, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		public static SelectStatement Average(this SelectStatement select, params Column[] columns)
		{
			if (columns.Any())
			{
				select.SourceFields.AddRange(columns.Select(cn => new Aggregate(AggregateType.Average, cn)));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Adds a list of columns to AVERAGE to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Average(this SelectStatement select, params string[] columnNames)
		{
			if (columnNames.Any())
			{
				select.SourceFields.AddRange(columnNames.Select(cn => new Aggregate(AggregateType.Average, new Column(cn))));
			}
			else
			{
				select.SourceFields.Add(new Aggregate(AggregateType.Average, new Column("*")));
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Distinct(this SelectStatement select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="skip">The number of records to skip.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Skip(this SelectStatement select, int skip)
		{
			select.StartIndex = skip;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="take">The number of records to take.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Take(this SelectStatement select, int take)
		{
			select.Limit = take;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value));
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Where(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.AddRange(conditions);
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds a NOT condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement WhereNot(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Not = true });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.And;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement And(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.And });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnName">Name of the column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, string columnName, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(columnName, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="column">The column.</param>
		/// <param name="op">The op.</param>
		/// <param name="value">The value.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, SourceExpression column, SqlOperator op, object value)
		{
			select.Conditions.Add(new Condition(column, op, value) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, ConditionExpression condition)
		{
			condition.Relationship = ConditionRelationship.Or;
			select.Conditions.Add(condition);
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="conditions">The conditions.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Or(this SelectStatement select, params ConditionExpression[] conditions)
		{
			select.Conditions.Add(new ConditionCollection(conditions) { Relationship = ConditionRelationship.Or });
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(cn => new OrderByExpression(cn)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params Column[] columns)
		{
			select.OrderByFields.AddRange(columns.Select(c => new OrderByExpression(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderBy(this SelectStatement select, params OrderByExpression[] columns)
		{
			select.OrderByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds a list of columns to order descendingly by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement OrderByDescending(this SelectStatement select, params string[] columnNames)
		{
			select.OrderByFields.AddRange(columnNames.Select(c => new OrderByExpression(c, OrderDirection.Descending)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columnNames">The column names.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params string[] columnNames)
		{
			select.GroupByFields.AddRange(columnNames.Select(c => new Column(c)));
			return select;
		}

		/// <summary>
		/// Adds a list of columns to group by to the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="columns">The columns.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement GroupBy(this SelectStatement select, params Column[] columns)
		{
			select.GroupByFields.AddRange(columns);
			return select;
		}

		/// <summary>
		/// Adds another statement to the select statement as a UNION.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="union">The union.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Union(this SelectStatement select, SelectStatement union)
		{
			select.UnionStatements.Add(union);
			return select;
		}

		/// <summary>
		/// Sets additional paths to include when loading the select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		/// <param name="path">The path.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement Include(this SelectStatement select, string path)
		{
			select.IncludePaths.Add(path);
			return select;
		}
	}

	/// <summary>
	/// The starting point for fluently creating select statements.
	/// </summary>
	public static partial class Select
	{
		/// <summary>
		/// Creates a select statement from a type corresponding to the table that records should be selected from.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="alias">The alias.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> From<T>(string alias = null)
		{
			return new SelectStatement<T>(alias);
		}

		/// <summary>
		/// Adds a list of columns to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		/// <exception cref="InvalidOperationException"></exception>
		public static SelectStatement<T> Columns<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			var field = FuncToPropertyInfo(property, true);
			if (field == null)
			{
				if (property.Body is NewExpression anonObject)
				{
					// It's a new anonymous object, so add each of its arguments
					foreach (var anonArg in anonObject.Arguments)
					{
						if (anonArg is MemberExpression mex)
						{
							select.SourceFields.Add((PropertyInfo)mex.Member);
						}
					}
				}
				else
				{
					throw new InvalidOperationException();
				}
			}
			else
			{
				select.SourceFields.Add(field);
			}
			return select;
		}

		/// <summary>
		/// Sets the select statement to select only DISTINCT records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Distinct<T>(this SelectStatement<T> select)
		{
			select.IsDistinct = true;
			return select;
		}

		/// <summary>
		/// Sets the select statement to count records matching the supplied condition.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select.And(condition);
		}

		/// <summary>
		/// Sets the select statement to count all records.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Count<T>(this SelectStatement<T> select)
		{
			select.AggregateFields.Add(new FieldAggregate(null, AggregateType.Count));
			return select;
		}

		/// <summary>
		/// Sets the select statement to sum the supplied properties.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Sum<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.AggregateFields.Add(new FieldAggregate(FuncToPropertyInfo(property), AggregateType.Sum));
			return select;
		}

		/// <summary>
		/// Sets the number of records to skip from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="startIndex">The start index.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Skip<T>(this SelectStatement<T> select, int startIndex)
		{
			select.StartIndex = startIndex;
			return select;
		}

		/// <summary>
		/// Sets the number of records to take from the start of the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="limit">The limit.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Take<T>(this SelectStatement<T> select, int limit)
		{
			select.Limit = limit;
			return select;
		}

		/// <summary>
		/// Adds a condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Where<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			return select.And(condition);
		}

		/// <summary>
		/// Adds an AND condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> And<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.AndAlso(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds an OR condition to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="condition">The condition.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> Or<T>(this SelectStatement<T> select, Expression<Func<T, bool>> condition)
		{
			if (select.Conditions != null)
			{
				var combined = select.Conditions.Body.OrElse(condition.Body);
				combined = AnonymousParameterReplacer.Replace(combined, condition.Parameters);
				select.Conditions = Expression.Lambda<Func<T, bool>>(combined, condition.Parameters);
			}
			else
			{
				select.Conditions = condition;
			}
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> OrderByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Ascending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to order descendingly by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> ThenByDescending<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.OrderByFields.Add(new FieldOrder(FuncToPropertyInfo(property), OrderDirection.Descending));
			return select;
		}

		/// <summary>
		/// Adds a list of properties to group by to the select statement.
		/// </summary>
		/// <typeparam name="T">The type corresponding to the table that records should be selected from.</typeparam>
		/// <param name="select">The select statement.</param>
		/// <param name="property">The property.</param>
		/// <returns>The select statement.</returns>
		public static SelectStatement<T> GroupBy<T>(this SelectStatement<T> select, Expression<Func<T, object>> property)
		{
			select.GroupByFields.Add(FuncToPropertyInfo(property));
			return select;
		}

		private static PropertyInfo FuncToPropertyInfo<T>(Expression<Func<T, object>> selector, bool returnNull = false)
		{
			if (selector.Body is MemberExpression mex)
			{
				return (PropertyInfo)mex.Member;
			}
			else if (selector.Body is UnaryExpression uex)
			{
				// Throw away converts
				if (uex.Operand is MemberExpression omex)
				{
					return (PropertyInfo)omex.Member;
				}
			}

			// HACK: Yes, this is ugly!
			if (returnNull)
			{
				return null;
			}
			else
			{
				throw new InvalidOperationException();
			}
		}
	}

	/// <summary>
	/// Builds command text and parameters from a statement for use in an SQL database.
	/// </summary>
	/// <seealso cref="CS.QueryBuilder.ICommandBuilder" />
	public class SqlCommandBuilder : ICommandBuilder
	{
		private const int IndentationWidth = 2;

		private enum Indentation
		{
			Same,
			Inner,
			Outer
		}

		/// <summary>
		/// Gets the command text.
		/// </summary>
		/// <value>
		/// The command text.
		/// </value>
		public StringBuilder CommandText { get; } = new StringBuilder();

		/// <summary>
		/// Gets the parameter values.
		/// </summary>
		/// <value>
		/// The parameter values.
		/// </value>
		public List<object> ParameterValues { get; } = new List<object>();

		private int Depth { get; set; }

		private bool IsNested { get; set; }

		/// <summary>
		/// Visits a statement and builds the command text and parameters.
		/// </summary>
		/// <param name="statement">The statement.</param>
		/// <param name="mapper">The mapper.</param>
		/// <exception cref="NotSupportedException"></exception>
		public void VisitStatement(Statement statement, DatabaseMapper mapper)
		{
			switch (statement.PartType)
			{
				case StatementPartType.Select:
				{
					VisitSelect((SelectStatement)statement);
					break;
				}
				case StatementPartType.GenericSelect:
				{
					var select = (SelectStatement)((GenericStatement)statement).CreateStatement(mapper);
					VisitSelect(select);
					break;
				}

				default:
				{
					// TODO:
					throw new NotSupportedException();
				}
			}
		}

		/// <summary>
		/// Visits a constant part.
		/// </summary>
		/// <param name="constant">The constant part.</param>
		protected virtual void VisitConstant(ConstantPart constant)
		{
			VisitObject(constant.Value);
			if (!string.IsNullOrEmpty(constant.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(constant.Alias);
				this.CommandText.Append("]");
			}
		}

		/// <summary>
		/// Visits an object.
		/// </summary>
		/// <param name="value">The object.</param>
		protected virtual void VisitObject(object value)
		{
			if (value == null)
			{
				this.CommandText.Append("NULL");
			}
			else if (value.GetType() == typeof(bool))
			{
				this.CommandText.Append(((bool)value) ? "1" : "0");
			}
			else if (value.GetType() == typeof(string) && value.ToString().Length == 0)
			{
				this.CommandText.Append("''");
			}
			else if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
			{
				var firstValue = true;
				foreach (var innerValue in enumerable)
				{
					if (!firstValue)
					{
						this.CommandText.Append(", ");
					}
					firstValue = false;
					if (innerValue is ConstantPart constantValue)
					{
						this.VisitConstant(constantValue);
					}
					else
					{
						this.VisitObject(innerValue);
					}
				}
			}
			else
			{
				var index = this.ParameterValues.IndexOf(value);
				if (index != -1)
				{
					this.CommandText.Append("@");
					this.CommandText.Append(index);
				}
				else
				{
					this.CommandText.Append("@");
					this.CommandText.Append(this.ParameterValues.Count);
					if (value.GetType().IsEnum)
					{
						this.ParameterValues.Add(Convert.ToInt64(value));
					}
					else
					{
						this.ParameterValues.Add(value);
					}
				}
			}
		}

		/// <summary>
		/// Visits a select statement.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelect(SelectStatement select)
		{
			// TODO: If we're using SQL Server 2012 we should just use the OFFSET keyword
			if (select.StartIndex > 0)
			{
				VisitSelectWithRowNumber(select);
				return;
			}

			if (select.IsAny)
			{
				VisitSelectWithAny(select);
				return;
			}

			if (select.IsAll)
			{
				VisitSelectWithAll(select);
				return;
			}

			if (select.IsContains)
			{
				VisitSelectWithContains(select);
				return;
			}

			// If any of the fields have aggregates that aren't grouped, remove the ordering as SQL Server doesn't like it
			// TODO: Only if they aren't grouped
			if (select.SourceFields.Any(f => f is Aggregate))
			{
				select.OrderByFields.Clear();
			}

			this.CommandText.Append("SELECT ");
			if (select.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and remove this method
				VisitLimitAtStart(select);
			}
			if (select.SourceFieldsFrom.Count > 0)
			{
				VisitSourceFieldsFrom(select);
			}
			if (select.SourceFields.Count > 0)
			{
				VisitSourceFields(select);
			}
			if (select.SourceFieldsFrom.Count == 0 && select.SourceFields.Count == 0)
			{
				if (this.IsNested)
				{
					// TODO: Rename tmp, it sucks
					this.CommandText.Append("NULL ");
					this.CommandText.Append("AS tmp");
				}
				else
				{
					// TODO: When to use "*" vs "NULL"?
					this.CommandText.Append("*");
				}
			}
			if (select.Source != null)
			{
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append("FROM ");
				this.VisitSource(select.Source);
			}
			if (select.SourceJoins != null)
			{
				for (var i = 0; i < select.SourceJoins.Count; i++)
				{
					this.AppendNewLine(Indentation.Same);
					this.VisitJoin(select.SourceJoins[i]);
				}
			}
			if (select.Conditions.Count > 0)
			{
				VisitConditions(select.Conditions);
			}
			if (select.GroupByFields.Count > 0)
			{
				VisitGroupBy(select);
			}
			if (select.OrderByFields.Count > 0 && !select.IsAggregate)
			{
				VisitOrderBy(select);
			}
			if (select.Limit > 0)
			{
				// TODO: Use OFFSET and FETCH for SQL Server and rename this method
				VisitLimitAtEnd(select);
			}
			foreach (var union in select.UnionStatements)
			{
				this.CommandText.AppendLine();
				this.CommandText.AppendLine("UNION ALL");
				VisitSelect(union);
			}
		}

		/// <summary>
		/// Visits a select statement with a row number.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithRowNumber(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT Fields
			// FROM (SELECT Fields,
			//		ROW_NUMBER() OVER (ORDER BY OrderFields) AS RowNumber
			//		FROM Table
			//		WHERE Condition)
			// WHERE RowNumber > Start AND RowNumber <= End
			// ORDER BY OrderFields

			// Clone the select and add the RowNumber field to it
			var inner = Select.From(select.Source);
			inner.SourceJoins.AddRange(select.SourceJoins);
			inner.Alias = "RowNumberTable";
			inner.SourceFields.AddRange(select.SourceFields);
			inner.SourceFields.Add(new RowNumber(select.OrderByFields.ToArray()));
			inner.Conditions.AddRange(select.Conditions);

			// If the original table selected all fields, we need to add another field to select them ourselves
			if (!select.SourceFields.Any())
			{
				if (select.Source is Table table)
				{
					inner.SourceFields.Add(new Column(table.Name, "*"));
				}
			}

			// Clone the select and change its source
			var outer = Select.From(inner);
			foreach (var field in select.SourceFields)
			{
				if (field is Column column)
				{
					outer.SourceFields.Add(new Column(inner.Alias, column.Name));
				}
			}
			if (select.StartIndex > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsGreaterThan, select.StartIndex));
			}
			if (select.Limit > 0)
			{
				outer.Conditions.Add(new Condition("RowNumber", SqlOperator.IsLessThanOrEqualTo, select.StartIndex + select.Limit));
			}
			outer.OrderByFields.Add(new OrderByExpression("RowNumber"));

			// Visit the outer select
			VisitSelect(outer);
		}

		/// <summary>
		/// Visits a select statement with ANY.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAny(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN EXISTS (");
			this.Indent(Indentation.Inner);

			select.IsAny = false;

			this.VisitSelect(select);

			select.IsAny = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with ALL.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithAll(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN NOT EXISTS (
			//		SELECT Fields
			//		FROM Table
			//		WHERE NOT Condition
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN NOT EXISTS (");
			this.Indent(Indentation.Inner);

			var not = select.Conditions.Not;
			select.IsAll = false;
			select.Conditions.Not = !not;

			this.VisitSelect(select);

			select.IsAll = true;
			select.Conditions.Not = not;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits a select with CONTAINS.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectWithContains(SelectStatement select)
		{
			// It's going to look something like this:
			// SELECT CASE WHEN @0 IN (
			//		SELECT Fields
			//		FROM Table
			// ) THEN 1 ELSE 0 END

			this.CommandText.Append("SELECT CASE WHEN ");
			this.VisitField(select.ContainsItem);
			this.CommandText.Append(" IN (");
			this.Indent(Indentation.Inner);

			select.IsContains = false;

			this.VisitSelect(select);

			select.IsContains = true;

			this.Indent(Indentation.Outer);
			this.CommandText.Append(") THEN 1 ELSE 0 END");
		}

		/// <summary>
		/// Visits an update statement.
		/// </summary>
		/// <param name="update">The update statement.</param>
		/// <exception cref="InvalidOperationException">An update statement must have at least one condition to avoid accidentally updating all data in a table</exception>
		//protected virtual void VisitUpdate(UpdateStatement update)
		//{
		//	this.CommandText.Append("UPDATE ");
		//	this.VisitTable(update.Target);
		//	this.CommandText.Append(" SET");
		//	this.AppendNewLine(Indentation.Inner);
		//	if (update.SetValues != null && update.SetValues.Count > 0)
		//	{
		//		for (var i = 0; i < update.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(",");
		//				this.AppendNewLine(Indentation.Same);
		//			}
		//			this.VisitColumn(update.SetValues[i].Column, ignoreTablePrefix: true);
		//			this.CommandText.Append(" = ");
		//			if (update.SetValues[i].Value is SelectStatement select)
		//			{
		//				// Special case - ensure a select statement is surrounded with brackets
		//				this.VisitField(new SelectExpression(select));
		//			}
		//			else
		//			{
		//				this.VisitField(update.SetValues[i].Value);
		//			}
		//		}
		//	}
		//	this.Indent(Indentation.Outer);
		//	if (update.Conditions != null && update.Conditions.Count > 0)
		//	{
		//		VisitConditions(update.Conditions);
		//	}
		//	else
		//	{
		//		throw new InvalidOperationException("An update statement must have at least one condition to avoid accidentally updating all data in a table");
		//	}
		//}

		/// <summary>
		/// Visits an insert statement.
		/// </summary>
		/// <param name="insert">The insert statement.</param>
		//protected virtual void VisitInsert(InsertStatement insert)
		//{
		//	this.CommandText.Append("INSERT INTO ");
		//	this.VisitTable(insert.Target);
		//	if (insert.SetValues != null && insert.SetValues.Count > 0)
		//	{
		//		this.CommandText.Append(" (");
		//		for (var i = 0; i < insert.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitColumn(insert.SetValues[i].Column);
		//		}
		//		this.CommandText.Append(")");
		//		this.AppendNewLine(Indentation.Same);
		//		this.CommandText.Append("VALUES (");
		//		for (var i = 0; i < insert.SetValues.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitField(insert.SetValues[i].Value);
		//		}
		//		this.CommandText.Append(")");
		//	}
		//	else if (insert.TargetFields != null && insert.TargetFields.Count > 0 && insert.Source != null)
		//	{
		//		this.CommandText.Append(" (");
		//		for (var i = 0; i < insert.TargetFields.Count; i++)
		//		{
		//			if (i > 0)
		//			{
		//				this.CommandText.Append(", ");
		//			}
		//			this.VisitColumn(insert.TargetFields[i]);
		//		}
		//		this.CommandText.Append(")");
		//		this.AppendNewLine(Indentation.Same);
		//		this.VisitSelect(insert.Source);
		//	}
		//	else
		//	{
		//		this.CommandText.Append(" DEFAULT VALUES");
		//	}
		//}

		/// <summary>
		/// Visits a delete statement.
		/// </summary>
		/// <param name="delete">The delete statement.</param>
		/// <exception cref="InvalidOperationException">A delete statement must have at least one condition to avoid accidentally deleting all data in a table</exception>
		//protected virtual void VisitDelete(DeleteStatement delete)
		//{
		//	this.CommandText.Append("DELETE FROM ");
		//	this.VisitTable(delete.Target);
		//	if (delete.Conditions != null && delete.Conditions.Count > 0)
		//	{
		//		VisitConditions(delete.Conditions);
		//	}
		//	else
		//	{
		//		throw new InvalidOperationException("A delete statement must have at least one condition to avoid accidentally deleting all data in a table");
		//	}
		//}

		/// <summary>
		/// Visits a source fields.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFields(SelectStatement select)
		{
			for (var i = 0; i < select.SourceFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.SourceFields[i]);
			}
		}

		/// <summary>
		/// Visits a source fields from.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSourceFieldsFrom(SelectStatement select)
		{
			// TODO: Should the SourceFieldsFrom actually be its own class?
			for (var i = 0; i < select.SourceFieldsFrom.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitTable(select.SourceFieldsFrom[i]);
				this.CommandText.Append(".*");
			}
			if (select.SourceFields.Count > 0)
			{
				this.CommandText.Append(", ");
			}
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="conditions">The condition collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditions(ConditionCollection conditions)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("WHERE ");
			if (conditions.Not)
			{
				this.CommandText.Append("NOT ");
			}
			for (var i = 0; i < conditions.Count; i++)
			{
				if (i > 0)
				{
					this.AppendNewLine(Indentation.Same);
					switch (conditions[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(conditions[i]);
			}
		}

		/// <summary>
		/// Visits a group by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitGroupBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("GROUP BY ");
			for (var i = 0; i < select.GroupByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.GroupByFields[i]);
			}
		}

		/// <summary>
		/// Visits an order by.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitOrderBy(SelectStatement select)
		{
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append("ORDER BY ");
			for (var i = 0; i < select.OrderByFields.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(select.OrderByFields[i].Expression);
				if (select.OrderByFields[i].Direction != OrderDirection.Ascending)
				{
					this.CommandText.Append(" DESC");
				}
			}
		}

		/// <summary>
		/// Visits a limit at start.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtStart(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a limit at end.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitLimitAtEnd(SelectStatement select)
		{
			// TODO: Is there a good default for this?
		}

		/// <summary>
		/// Visits a field.
		/// </summary>
		/// <param name="field">The field.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitField(StatementPart field)
		{
			switch (field.PartType)
			{
				case StatementPartType.Column:
				{
					this.VisitColumn((Column)field);
					break;
				}
				case StatementPartType.RowNumber:
				{
					this.VisitRowNumber((RowNumber)field);
					break;
				}
				case StatementPartType.Aggregate:
				{
					this.VisitAggregate((Aggregate)field);
					break;
				}
				case StatementPartType.ConditionalCase:
				{
					this.VisitConditionalCase((ConditionalCase)field);
					break;
				}
				case StatementPartType.ConditionPredicate:
				{
					this.VisitConditionPredicate((ConditionPredicate)field);
					break;
				}
				case StatementPartType.Exists:
				{
					this.VisitExists((Exists)field);
					break;
				}
				case StatementPartType.CoalesceFunction:
				{
					this.VisitCoalesceFunction((CoalesceFunction)field);
					break;
				}
				case StatementPartType.ConvertFunction:
				{
					this.VisitConvertFunction((ConvertFunction)field);
					break;
				}
				case StatementPartType.StringLengthFunction:
				{
					this.VisitStringLengthFunction((StringLengthFunction)field);
					break;
				}
				case StatementPartType.SubstringFunction:
				{
					this.VisitSubstringFunction((SubstringFunction)field);
					break;
				}
				case StatementPartType.StringRemoveFunction:
				{
					this.VisitStringRemoveFunction((StringRemoveFunction)field);
					break;
				}
				case StatementPartType.StringIndexFunction:
				{
					this.VisitStringCharIndexFunction((StringIndexFunction)field);
					break;
				}
				case StatementPartType.StringToUpperFunction:
				{
					this.VisitStringToUpperFunction((StringToUpperFunction)field);
					break;
				}
				case StatementPartType.StringToLowerFunction:
				{
					this.VisitStringToLowerFunction((StringToLowerFunction)field);
					break;
				}
				case StatementPartType.StringReplaceFunction:
				{
					this.VisitStringReplaceFunction((StringReplaceFunction)field);
					break;
				}
				case StatementPartType.StringTrimFunction:
				{
					this.VisitStringTrimFunction((StringTrimFunction)field);
					break;
				}
				case StatementPartType.StringCompareFunction:
				{
					this.VisitStringCompareFunction((StringCompareFunction)field);
					break;
				}
				case StatementPartType.StringConcatenateFunction:
				{
					this.VisitStringConcatenateFunction((StringConcatenateFunction)field);
					break;
				}
				case StatementPartType.DatePartFunction:
				{
					this.VisitDatePartFunction((DatePartFunction)field);
					break;
				}
				case StatementPartType.DateAddFunction:
				{
					this.VisitDateAddFunction((DateAddFunction)field);
					break;
				}
				case StatementPartType.DateNewFunction:
				{
					this.VisitDateNewFunction((DateNewFunction)field);
					break;
				}
				case StatementPartType.DateDifferenceFunction:
				{
					this.VisitDateDifferenceFunction((DateDifferenceFunction)field);
					break;
				}
				case StatementPartType.NumberAbsoluteFunction:
				{
					this.VisitNumberAbsoluteFunction((NumberAbsoluteFunction)field);
					break;
				}
				case StatementPartType.NumberNegateFunction:
				{
					this.VisitNumberNegateFunction((NumberNegateFunction)field);
					break;
				}
				case StatementPartType.NumberCeilingFunction:
				{
					this.VisitNumberCeilingFunction((NumberCeilingFunction)field);
					break;
				}
				case StatementPartType.NumberFloorFunction:
				{
					this.VisitNumberFloorFunction((NumberFloorFunction)field);
					break;
				}
				case StatementPartType.NumberRoundFunction:
				{
					this.VisitNumberRoundFunction((NumberRoundFunction)field);
					break;
				}
				case StatementPartType.NumberTruncateFunction:
				{
					this.VisitNumberTruncateFunction((NumberTruncateFunction)field);
					break;
				}
				case StatementPartType.NumberSignFunction:
				{
					this.VisitNumberSignFunction((NumberSignFunction)field);
					break;
				}
				case StatementPartType.NumberPowerFunction:
				{
					this.VisitNumberPowerFunction((NumberPowerFunction)field);
					break;
				}
				case StatementPartType.NumberRootFunction:
				{
					this.VisitNumberRootFunction((NumberRootFunction)field);
					break;
				}
				case StatementPartType.NumberExponentialFunction:
				{
					this.VisitNumberExponentialFunction((NumberExponentialFunction)field);
					break;
				}
				case StatementPartType.NumberLogFunction:
				{
					this.VisitNumberLogFunction((NumberLogFunction)field);
					break;
				}
				case StatementPartType.NumberLog10Function:
				{
					this.VisitNumberLog10Function((NumberLog10Function)field);
					break;
				}
				case StatementPartType.NumberTrigFunction:
				{
					this.VisitNumberTrigFunction((NumberTrigFunction)field);
					break;
				}
				case StatementPartType.BinaryOperation:
				{
					this.VisitBinaryOperation((BinaryOperation)field);
					break;
				}
				case StatementPartType.UnaryOperation:
				{
					this.VisitUnaryOperation((UnaryOperation)field);
					break;
				}
				case StatementPartType.LiteralPart:
				{
					this.VisitLiteralPart((LiteralPart)field);
					break;
				}
				case StatementPartType.Select:
				{
					this.VisitSelect((SelectStatement)field);
					break;
				}
				case StatementPartType.ConstantPart:
				{
					this.VisitConstant((ConstantPart)field);
					break;
				}
				case StatementPartType.Condition:
				{
					this.VisitCondition((Condition)field);
					break;
				}
				case StatementPartType.FieldCollection:
				{
					var collection = (FieldCollection)field;
					for (var i = 0; i < collection.Count; i++)
					{
						if (i > 0)
						{
							this.CommandText.Append(", ");
						}
						this.VisitField(collection[i]);
					}
					break;
				}
				case StatementPartType.SelectExpression:
				{
					this.VisitSelectExpression((SelectExpression)field);
					break;
				}
				default:
				{
					// TODO: Words for all exceptions
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a column.
		/// </summary>
		/// <param name="column">The column.</param>
		/// <param name="ignoreTablePrefix">if set to <c>true</c> [ignore table prefix].</param>
		protected virtual void VisitColumn(Column column, bool ignoreTablePrefix = false)
		{
			if (!ignoreTablePrefix && column.Table != null && !string.IsNullOrEmpty(column.Table.Name))
			{
				if (!string.IsNullOrEmpty(column.Table.Alias))
				{
					this.CommandText.Append("[");
					this.CommandText.Append(column.Table.Alias);
					this.CommandText.Append("]");
				}
				else
				{
					VisitTable(column.Table);
				}
				this.CommandText.Append(".");
			}

			if (column.Name == "*")
			{
				this.CommandText.Append("*");
			}
			else if (column.Name.StartsWith("@"))
			{
				// HACK: Allowing the user to pass parameter names in with new Column("@ParameterID")
				// but it might be better to require new Parameter("@ParameterID")
				this.CommandText.Append(column.Name);
			}
			else
			{
				this.CommandText.Append("[");
				this.CommandText.Append(column.Name);
				this.CommandText.Append("]");
				if (!string.IsNullOrEmpty(column.Alias))
				{
					this.CommandText.Append(" AS [");
					this.CommandText.Append(column.Alias);
					this.CommandText.Append("]");
				}
			}
		}

		/// <summary>
		/// Visits a source.
		/// </summary>
		/// <param name="source">The source.</param>
		/// <exception cref="InvalidOperationException">Select source is not valid type</exception>
		protected virtual void VisitSource(StatementPart source)
		{
			var previousIsNested = this.IsNested;
			this.IsNested = true;
			switch (source.PartType)
			{
				case StatementPartType.Table:
				{
					var table = (Table)source;
					this.VisitTable(table);
					if (!string.IsNullOrEmpty(table.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(table.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				case StatementPartType.Select:
				{
					var select = (SelectStatement)source;
					this.CommandText.Append("(");
					this.AppendNewLine(Indentation.Inner);
					this.VisitSelect(select);
					this.AppendNewLine(Indentation.Same);
					this.CommandText.Append(")");
					if (!string.IsNullOrEmpty(select.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(select.Alias);
						this.CommandText.Append("]");
					}
					this.Indent(Indentation.Outer);
					break;
				}
				case StatementPartType.Join:
				{
					this.VisitJoin((Join)source);
					break;
				}
				case StatementPartType.UserDefinedFunction:
				{
					var function = (UserDefinedFunction)source;
					this.VisitUserDefinedFunction(function);
					if (!string.IsNullOrEmpty(function.Alias))
					{
						this.CommandText.Append(" AS [");
						this.CommandText.Append(function.Alias);
						this.CommandText.Append("]");
					}
					break;
				}
				default:
				{
					throw new InvalidOperationException("Select source is not valid type");
				}
			}
			this.IsNested = previousIsNested;
		}

		/// <summary>
		/// Visits a table.
		/// </summary>
		/// <param name="table">The table.</param>
		protected virtual void VisitTable(Table table)
		{
			if (!string.IsNullOrEmpty(table.Schema))
			{
				this.CommandText.Append("[");
				this.CommandText.Append(table.Schema);
				this.CommandText.Append("]");
				this.CommandText.Append(".");
			}
			this.CommandText.Append("[");
			this.CommandText.Append(table.Name);
			this.CommandText.Append("]");
		}

		/// <summary>
		/// Visits an user defined function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitUserDefinedFunction(UserDefinedFunction function)
		{
			if (!string.IsNullOrEmpty(function.Schema))
			{
				this.CommandText.Append(function.Schema);
				this.CommandText.Append(".");
			}
			this.CommandText.Append(function.Name);
			this.CommandText.Append("(");
			for (var i = 0; i < function.Parameters.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitObject(function.Parameters[i].Value);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a join.
		/// </summary>
		/// <param name="join">The join.</param>
		protected virtual void VisitJoin(Join join)
		{
			switch (join.JoinType)
			{
				case JoinType.Inner:
				{
					this.CommandText.Append("INNER JOIN ");
					break;
				}
				case JoinType.Left:
				{
					this.CommandText.Append("LEFT OUTER JOIN ");
					break;
				}
				case JoinType.Right:
				{
					this.CommandText.Append("RIGHT OUTER JOIN ");
					break;
				}
				case JoinType.Cross:
				{
					this.CommandText.Append("CROSS JOIN ");
					break;
				}
				case JoinType.CrossApply:
				{
					this.CommandText.Append("CROSS APPLY ");
					break;
				}
			}
			this.VisitSource(join.Table);
			if (join.Conditions.Count > 0)
			{
				this.CommandText.Append(" ON ");
				this.VisitConditionCollection(join.Conditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitCondition(ConditionExpression condition)
		{
			// TODO: Should all types of conditions be a class?  Not exposed to the user, because that
			// interface would be gross
			if (condition is Exists existsCondition)
			{
				VisitExists(existsCondition);
				return;
			}

			if (condition.Not)
			{
				this.CommandText.Append("NOT ");
			}

			if (condition is Condition singleCondition)
			{
				VisitCondition(singleCondition);
			}
			else if (condition is ConditionCollection multipleConditions)
			{
				VisitConditionCollection(multipleConditions);
			}
		}

		/// <summary>
		/// Visits a condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		/// <exception cref="InvalidOperationException">Invalid operator: " + condition.Operator</exception>
		protected virtual void VisitCondition(Condition condition)
		{
			// Check for null comparisons first
			var fieldIsNull = (condition.Field is ConstantPart constantField && constantField.Value == null);
			var valueIsNull = (condition.Value is ConstantPart constantValue && constantValue.Value == null);
			if ((condition.Operator == SqlOperator.Equals || condition.Operator == SqlOperator.NotEquals) &&
				(fieldIsNull || valueIsNull))
			{
				if (fieldIsNull)
				{
					this.VisitField(condition.Value);
				}
				else if (valueIsNull)
				{
					this.VisitField(condition.Field);
				}
				if (condition.Operator == SqlOperator.Equals)
				{
					this.CommandText.Append(" IS NULL");
				}
				else if (condition.Operator == SqlOperator.NotEquals)
				{
					this.CommandText.Append(" IS NOT NULL");
				}
			}
			else
			{
				switch (condition.Operator)
				{
					case SqlOperator.Equals:
					{
						VisitEqualsCondition(condition);
						break;
					}
					case SqlOperator.NotEquals:
					{
						VisitNotEqualsCondition(condition);
						break;
					}
					case SqlOperator.IsLessThan:
					{
						VisitIsLessThanCondition(condition);
						break;
					}
					case SqlOperator.IsLessThanOrEqualTo:
					{
						VisitIsLessThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThan:
					{
						VisitIsGreaterThanCondition(condition);
						break;
					}
					case SqlOperator.IsGreaterThanOrEqualTo:
					{
						VisitIsGreaterThanOrEqualToCondition(condition);
						break;
					}
					case SqlOperator.IsIn:
					{
						VisitIsInCondition(condition);
						break;
					}
					case SqlOperator.Contains:
					{
						VisitContainsCondition(condition);
						break;
					}
					case SqlOperator.StartsWith:
					{
						VisitStartsWithCondition(condition);
						break;
					}
					case SqlOperator.EndsWith:
					{
						VisitEndsWithCondition(condition);
						break;
					}
					default:
					{
						throw new InvalidOperationException("Invalid operator: " + condition.Operator);
					}
				}
			}
		}

		/// <summary>
		/// Visits an equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" = ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a not equals condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitNotEqualsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <> ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" < ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is less than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsLessThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" <= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" > ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is greater than or equal to condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsGreaterThanOrEqualToCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" >= ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits an is in condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitIsInCondition(Condition condition)
		{
			// If it's in an empty list, just check against false
			var handled = false;
			if (condition.Value.PartType == StatementPartType.ConstantPart)
			{
				var value = ((ConstantPart)condition.Value).Value;
				if (value is IEnumerable enumerable && !(value is string) && !(value is byte[]))
				{
					// HACK: Ugh
					var hasThings = false;
					foreach (var thing in enumerable)
					{
						hasThings = true;
						break;
					}
					if (!hasThings)
					{
						handled = true;
						this.CommandText.Append(" 0 <> 0");
					}
				}
			}
			if (!handled)
			{
				this.VisitField(condition.Field);
				this.CommandText.Append(" IN (");
				this.AppendNewLine(Indentation.Inner);
				this.VisitField(condition.Value);
				this.AppendNewLine(Indentation.Same);
				this.CommandText.Append(")");
				this.AppendNewLine(Indentation.Outer);
			}
		}

		/// <summary>
		/// Visits a contains condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitContainsCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits a starts with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitStartsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE ");
			this.VisitField(condition.Value);
			this.CommandText.Append(" + '%'");
		}

		/// <summary>
		/// Visits an ends with condition.
		/// </summary>
		/// <param name="condition">The condition.</param>
		protected virtual void VisitEndsWithCondition(Condition condition)
		{
			this.VisitField(condition.Field);
			this.CommandText.Append(" LIKE '%' + ");
			this.VisitField(condition.Value);
		}

		/// <summary>
		/// Visits a condition collection.
		/// </summary>
		/// <param name="collection">The collection.</param>
		/// <exception cref="InvalidOperationException"></exception>
		protected virtual void VisitConditionCollection(ConditionCollection collection)
		{
			this.CommandText.Append("(");
			for (var i = 0; i < collection.Count; i++)
			{
				if (i > 0)
				{
					// TODO: make this a visitrelationship method
					this.AppendNewLine(Indentation.Same);
					switch (collection[i].Relationship)
					{
						case ConditionRelationship.And:
						{
							this.CommandText.Append(" AND ");
							break;
						}
						case ConditionRelationship.Or:
						{
							this.CommandText.Append(" OR ");
							break;
						}
						default:
						{
							throw new InvalidOperationException();
						}
					}
				}
				this.VisitCondition(collection[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a conditional case.
		/// </summary>
		/// <param name="conditional">The conditional.</param>
		protected virtual void VisitConditionalCase(ConditionalCase conditional)
		{
			if (conditional.Test is Condition)
			{
				this.CommandText.Append("(CASE WHEN ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" THEN ");
				this.VisitField(conditional.IfTrue);
				var ifFalse = conditional.IfFalse;
				while (ifFalse != null && ifFalse.PartType == StatementPartType.ConditionalCase)
				{
					var subconditional = (ConditionalCase)conditional.IfFalse;
					this.CommandText.Append(" WHEN ");
					this.VisitField(subconditional.Test);
					this.CommandText.Append(" THEN ");
					this.VisitField(subconditional.IfTrue);
					ifFalse = subconditional.IfFalse;
				}
				if (ifFalse != null)
				{
					this.CommandText.Append(" ELSE ");
					this.VisitField(ifFalse);
				}
				this.CommandText.Append(" END)");
			}
			else
			{
				this.CommandText.Append("(CASE ");
				this.VisitField(conditional.Test);
				this.CommandText.Append(" WHEN 0 THEN ");
				this.VisitField(conditional.IfFalse);
				this.CommandText.Append(" ELSE ");
				this.VisitField(conditional.IfTrue);
				this.CommandText.Append(" END)");
			}
		}

		/// <summary>
		/// Visits a row number.
		/// </summary>
		/// <param name="rowNumber">The row number.</param>
		protected virtual void VisitRowNumber(RowNumber rowNumber)
		{
			this.CommandText.Append("ROW_NUMBER() OVER(");
			if (rowNumber.OrderByFields != null && rowNumber.OrderByFields.Count > 0)
			{
				this.CommandText.Append("ORDER BY ");
				for (var i = 0; i < rowNumber.OrderByFields.Count; i++)
				{
					if (i > 0)
					{
						this.CommandText.Append(", ");
					}
					this.VisitField(rowNumber.OrderByFields[i].Expression);
					if (rowNumber.OrderByFields[i].Direction != OrderDirection.Ascending)
					{
						this.CommandText.Append(" DESC");
					}
				}
			}
			this.CommandText.Append(") AS RowNumber");
		}

		/// <summary>
		/// Visits an aggregate.
		/// </summary>
		/// <param name="aggregate">The aggregate.</param>
		protected virtual void VisitAggregate(Aggregate aggregate)
		{
			this.CommandText.Append(GetAggregateName(aggregate.AggregateType));
			this.CommandText.Append("(");
			if (aggregate.IsDistinct)
			{
				this.CommandText.Append("DISTINCT ");
			}
			if (aggregate.Field != null)
			{
				this.VisitField(aggregate.Field);
			}
			else if (aggregate.AggregateType == AggregateType.Count ||
				aggregate.AggregateType == AggregateType.BigCount)
			{
				this.CommandText.Append("*");
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Gets the name of the aggregate.
		/// </summary>
		/// <param name="aggregateType">Type of the aggregate.</param>
		/// <returns></returns>
		/// <exception cref="Exception">Unknown aggregate type: {aggregateType}</exception>
		private string GetAggregateName(AggregateType aggregateType)
		{
			switch (aggregateType)
			{
				case AggregateType.Count:
				{
					return "COUNT";
				}
				case AggregateType.BigCount:
				{
					return "COUNT_BIG";
				}
				case AggregateType.Min:
				{
					return "MIN";
				}
				case AggregateType.Max:
				{
					return "MAX";
				}
				case AggregateType.Sum:
				{
					return "SUM";
				}
				case AggregateType.Average:
				{
					return "AVG";
				}
				default:
				{
					throw new Exception($"Unknown aggregate type: {aggregateType}");
				}
			}
		}

		/// <summary>
		/// Visits a condition predicate.
		/// </summary>
		/// <param name="predicate">The predicate.</param>
		protected virtual void VisitConditionPredicate(ConditionPredicate predicate)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(predicate.Predicate);
			this.CommandText.Append(" THEN 1 ELSE 0 END)");
		}

		/// <summary>
		/// Visits an exists.
		/// </summary>
		/// <param name="exists">The exists.</param>
		protected virtual void VisitExists(Exists exists)
		{
			if (exists.Not)
			{
				this.CommandText.Append("NOT ");
			}
			this.CommandText.Append("EXISTS (");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(exists.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			this.Indent(Indentation.Outer);
		}

		/// <summary>
		/// Visits a coalesce function.
		/// </summary>
		/// <param name="coalesce">The coalesce.</param>
		protected virtual void VisitCoalesceFunction(CoalesceFunction coalesce)
		{
			StatementPart first = coalesce.Arguments[0];
			StatementPart second = coalesce.Arguments[1];

			this.CommandText.Append("COALESCE(");
			this.VisitField(first);
			this.CommandText.Append(", ");
			while (second.PartType == StatementPartType.CoalesceFunction)
			{
				var secondCoalesce = (CoalesceFunction)second;
				this.VisitField(secondCoalesce.Arguments[0]);
				this.CommandText.Append(", ");
				second = secondCoalesce.Arguments[1];
			}
			this.VisitField(second);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a function.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="arguments">The arguments.</param>
		protected virtual void VisitFunction(string name, params StatementPart[] arguments)
		{
			this.CommandText.Append(name);
			this.CommandText.Append("(");
			for (var i = 0; i < arguments.Length; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(", ");
				}
				this.VisitField(arguments[i]);
			}
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a convert function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitConvertFunction(ConvertFunction function)
		{
			// TODO: Handle more types
			this.CommandText.Append("CONVERT(VARCHAR, ");
			this.VisitField(function.Expression);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string length function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringLengthFunction(StringLengthFunction function)
		{
			VisitFunction("LEN", function.Argument);
		}

		/// <summary>
		/// Visits a substring function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitSubstringFunction(SubstringFunction function)
		{
			this.CommandText.Append("SUBSTRING(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(")");
		}

		/// <summary>
		/// Visits a string remove function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringRemoveFunction(StringRemoveFunction function)
		{
			this.CommandText.Append("STUFF(");
			this.VisitField(function.Argument);
			this.CommandText.Append(", ");
			this.VisitField(function.StartIndex);
			this.CommandText.Append(" + 1, ");
			this.VisitField(function.Length);
			this.CommandText.Append(", '')");
		}

		/// <summary>
		/// Visits a string character index function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCharIndexFunction(StringIndexFunction function)
		{
			this.CommandText.Append("(");
			if (function.StartIndex != null)
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument, function.StartIndex);
			}
			else
			{
				this.VisitFunction("CHARINDEX", function.StringToFind, function.Argument);
			}
			this.CommandText.Append(" - 1)");
		}

		/// <summary>
		/// Visits a string to upper function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToUpperFunction(StringToUpperFunction function)
		{
			VisitFunction("UPPER", function.Argument);
		}

		/// <summary>
		/// Visits a string to lower function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringToLowerFunction(StringToLowerFunction function)
		{
			VisitFunction("LOWER", function.Argument);
		}

		/// <summary>
		/// Visits a string replace function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringReplaceFunction(StringReplaceFunction function)
		{
			VisitFunction("REPLACE", function.Argument, function.OldValue, function.NewValue);
		}

		/// <summary>
		/// Visits a string trim function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringTrimFunction(StringTrimFunction function)
		{
			this.CommandText.Append("RTRIM(LTRIM(");
			this.VisitField(function.Argument);
			this.CommandText.Append("))");
		}

		/// <summary>
		/// Visits a string compare function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringCompareFunction(StringCompareFunction function)
		{
			this.CommandText.Append("(CASE WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" = ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN 0 WHEN ");
			this.VisitField(function.Argument);
			this.CommandText.Append(" < ");
			this.VisitField(function.Other);
			this.CommandText.Append(" THEN -1 ELSE 1 END)");
		}

		/// <summary>
		/// Visits a string concatenate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitStringConcatenateFunction(StringConcatenateFunction function)
		{
			for (var i = 0; i < function.Arguments.Count; i++)
			{
				if (i > 0)
				{
					this.CommandText.Append(" + ");
				}
				this.VisitField(function.Arguments[i]);
			}
		}

		/// <summary>
		/// Visits a date part function.
		/// </summary>
		/// <param name="function">The function.</param>
		/// <exception cref="InvalidOperationException">Invalid date part: " + function.DatePart</exception>
		protected virtual void VisitDatePartFunction(DatePartFunction function)
		{
			switch (function.DatePart)
			{
				case DatePart.Millisecond:
				case DatePart.Second:
				case DatePart.Minute:
				case DatePart.Hour:
				case DatePart.Day:
				case DatePart.Month:
				case DatePart.Year:
				{
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
						function.Argument
					});
					break;
				}
				case DatePart.DayOfWeek:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("weekday"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.DayOfYear:
				{
					this.CommandText.Append("(");
					this.VisitFunction("DATEPART", new StatementPart[]
					{
						new LiteralPart("dayofyear"),
						function.Argument
					});
					this.CommandText.Append(" - 1)");
					break;
				}
				case DatePart.Date:
				{
					this.CommandText.Append("DATEADD(dd, DATEDIFF(dd, 0, ");
					this.VisitField(function.Argument);
					this.CommandText.Append("), 0)");
					break;
				}
				default:
				{
					throw new InvalidOperationException("Invalid date part: " + function.DatePart);
				}
			}
		}

		/// <summary>
		/// Visits a date add function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateAddFunction(DateAddFunction function)
		{
			this.VisitFunction("DATEADD", new StatementPart[]
			{
				new LiteralPart(function.DatePart.ToString().ToLowerInvariant()),
				function.Number,
				function.Argument
			});
		}

		/// <summary>
		/// Visits a date new function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateNewFunction(DateNewFunction function)
		{
			if (function.Hour != null)
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append(") + ' ' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Hour);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Minute);
				this.CommandText.Append(") + ':' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Second);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("CONVERT(DATETIME, ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Year);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Month);
				this.CommandText.Append(") + '/' + ");
				this.CommandText.Append("CONVERT(NVARCHAR, ");
				this.VisitField(function.Day);
				this.CommandText.Append("))");
			}

		}

		/// <summary>
		/// Visits a date difference function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitDateDifferenceFunction(DateDifferenceFunction function)
		{
			this.VisitFunction("DATEDIFF", function.Date1, function.Date2);
		}

		/// <summary>
		/// Visits a number absolute function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberAbsoluteFunction(NumberAbsoluteFunction function)
		{
			this.VisitFunction("ABS", function.Argument);
		}

		/// <summary>
		/// Visits a number negate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberNegateFunction(NumberNegateFunction function)
		{
			this.CommandText.Append("-");
			this.VisitField(function.Argument);
		}

		/// <summary>
		/// Visits a number ceiling function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberCeilingFunction(NumberCeilingFunction function)
		{
			this.VisitFunction("CEILING", function.Argument);
		}

		/// <summary>
		/// Visits a number floor function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberFloorFunction(NumberFloorFunction function)
		{
			this.VisitFunction("FLOOR", function.Argument);
		}

		/// <summary>
		/// Visits a number round function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRoundFunction(NumberRoundFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, function.Precision);
		}

		/// <summary>
		/// Visits a number truncate function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTruncateFunction(NumberTruncateFunction function)
		{
			this.VisitFunction("ROUND", function.Argument, new ConstantPart(0), new ConstantPart(1));
		}

		/// <summary>
		/// Visits a number sign function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberSignFunction(NumberSignFunction function)
		{
			this.VisitFunction("SIGN", function.Argument);
		}

		/// <summary>
		/// Visits a number power function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberPowerFunction(NumberPowerFunction function)
		{
			this.VisitFunction("POWER", function.Argument, function.Power);
		}

		/// <summary>
		/// Visits a number root function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberRootFunction(NumberRootFunction function)
		{
			// TODO: I'm being lazy, if root > 3 then we should to convert it to POW(argument, 1 / root)
			this.VisitFunction("SQRT", function.Argument);
		}

		/// <summary>
		/// Visits a number exponential function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberExponentialFunction(NumberExponentialFunction function)
		{
			this.VisitFunction("EXP", function.Argument);
		}

		/// <summary>
		/// Visits a number log function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLogFunction(NumberLogFunction function)
		{
			this.VisitFunction("LOG", function.Argument);
		}

		/// <summary>
		/// Visits a number log10 function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberLog10Function(NumberLog10Function function)
		{
			this.VisitFunction("LOG10", function.Argument);
		}

		/// <summary>
		/// Visits a number trig function.
		/// </summary>
		/// <param name="function">The function.</param>
		protected virtual void VisitNumberTrigFunction(NumberTrigFunction function)
		{
			if (function.Argument2 != null)
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument, function.Argument2);
			}
			else
			{
				this.VisitFunction(function.Function.ToString().ToUpperInvariant(), function.Argument);
			}
		}

		/// <summary>
		/// Visits a binary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitBinaryOperation(BinaryOperation operation)
		{
			if (operation.Operator == BinaryOperator.LeftShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" * POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else if (operation.Operator == BinaryOperator.RightShift)
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" / POWER(2, ");
				this.VisitField(operation.Right);
				this.CommandText.Append("))");
			}
			else
			{
				this.CommandText.Append("(");
				this.VisitField(operation.Left);
				this.CommandText.Append(" ");
				this.CommandText.Append(GetOperatorName(operation.Operator));
				this.CommandText.Append(" ");
				this.VisitField(operation.Right);
				this.CommandText.Append(")");
			}
		}

		private string GetOperatorName(BinaryOperator op)
		{
			switch (op)
			{
				case BinaryOperator.Add:
				{
					return "+";
				}
				case BinaryOperator.Subtract:
				{
					return "-";
				}
				case BinaryOperator.Multiply:
				{
					return "*";
				}
				case BinaryOperator.Divide:
				{
					return "/";
				}
				case BinaryOperator.Remainder:
				{
					return "%";
				}
				case BinaryOperator.ExclusiveOr:
				{
					return "^";
				}
				case BinaryOperator.LeftShift:
				{
					return "<<";
				}
				case BinaryOperator.RightShift:
				{
					return ">>";
				}
				case BinaryOperator.BitwiseAnd:
				{
					return "&";
				}
				case BinaryOperator.BitwiseOr:
				case BinaryOperator.BitwiseExclusiveOr:
				{
					return "|";
				}
				case BinaryOperator.BitwiseNot:
				{
					return "~";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a unary operation.
		/// </summary>
		/// <param name="operation">The operation.</param>
		protected virtual void VisitUnaryOperation(UnaryOperation operation)
		{
			this.CommandText.Append(GetOperatorName(operation.Operator));
			// TODO: If isbinary: this.Builder.Append(" ");
			this.VisitField(operation.Expression);
		}

		private string GetOperatorName(UnaryOperator op)
		{
			switch (op)
			{
				case UnaryOperator.Not:
				{
					// TODO: return IsBoolean(unary.Expression.Type) ? "NOT" : "~";
					return "NOT ";
				}
				case UnaryOperator.Negate:
				{
					return "-";
				}
				default:
				{
					throw new InvalidOperationException();
				}
			}
		}

		/// <summary>
		/// Visits a literal part.
		/// </summary>
		/// <param name="literalPart">The literal part.</param>
		protected virtual void VisitLiteralPart(LiteralPart literalPart)
		{
			this.CommandText.Append(literalPart.Value);
		}

		/// <summary>
		/// Visits a select expression.
		/// </summary>
		/// <param name="select">The select statement.</param>
		protected virtual void VisitSelectExpression(SelectExpression select)
		{
			this.CommandText.Append("(");
			this.AppendNewLine(Indentation.Inner);
			this.VisitSelect(select.Select);
			this.AppendNewLine(Indentation.Same);
			this.CommandText.Append(")");
			if (!string.IsNullOrEmpty(select.Alias))
			{
				this.CommandText.Append(" AS [");
				this.CommandText.Append(select.Alias);
				this.CommandText.Append("]");
			}
			this.Indent(Indentation.Outer);
		}

		private void AppendNewLine(Indentation style)
		{
			this.CommandText.AppendLine();
			this.Indent(style);
			for (var i = 0; i < this.Depth * IndentationWidth; i++)
			{
				this.CommandText.Append(" ");
			}
		}

		private void Indent(Indentation style)
		{
			if (style == Indentation.Inner)
			{
				this.Depth += 1;
			}
			else if (style == Indentation.Outer)
			{
				this.Depth -= 1;
				System.Diagnostics.Debug.Assert(this.Depth >= 0);
			}
		}
	}

	/// <summary>
	/// Converts QueryModels into Select statements for passing to the database.
	/// </summary>
	public class StatementCreator : QueryModelVisitorBase
	{
		private DatabaseMapper Configuration { get; set; }

        private bool AliasTables { get; set; }

		private SelectStatement SelectStatement { get; set; }

		private StatementCreator(DatabaseMapper mapper, bool aliasTables)
		{
			this.Configuration = mapper;
            this.AliasTables = aliasTables;
			this.SelectStatement = new SelectStatement();
		}

		/// <summary>
		/// Visits the specified query model.
		/// </summary>
		/// <param name="queryModel">The query model.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static SelectStatement Visit(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementCreator(mapper, aliasTables);
			queryModel.Accept(visitor);
			return visitor.SelectStatement;
		}

		/// <summary>
		/// Visits the statement conditions.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="conditions">The conditions.</param>
		/// <param name="mapper">The mapper.</param>
		/// <param name="aliasTables">if set to <c>true</c> [alias tables].</param>
		/// <returns></returns>
		public static ConditionCollection VisitStatementConditions<T>(Expression<Func<T, bool>> conditions, DatabaseMapper mapper, bool aliasTables)
		{
			// Build a new query
			var queryParser = QueryParser.CreateDefault();
			var queryExecutor = new StatementExecutor();
			var query = new StatementQuery<T>(queryParser, queryExecutor);

			// Create an expression to select from the query with the conditions so that we have a sequence for Re-Linq to parse
			var expression = Expression.Call(
				typeof(Queryable),
				"Where",
				new Type[] { query.ElementType },
				query.Expression,
				conditions);

			// Parse the expression with Re-Linq
			var queryModel = queryParser.GetParsedQuery(expression);

			// Get the conditions from the query model
			var visitor = new StatementCreator(mapper, aliasTables);
			visitor.SelectStatement = new SelectStatement();
			queryModel.Accept(visitor);
			return visitor.SelectStatement.Conditions;
		}

		/// <summary>
		/// Visits the select clause.
		/// </summary>
		/// <param name="selectClause">The select clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
		{
			if (selectClause.Selector.NodeType != ExpressionType.Extension)
			{
				var fields = StatementPartCreator.Visit(queryModel, selectClause.Selector, this.Configuration, this.AliasTables);
				this.SelectStatement.SourceFields.Add((SourceExpression)fields);
			}

			base.VisitSelectClause(selectClause, queryModel);
		}

		/// <summary>
		/// Visits the main from clause.
		/// </summary>
		/// <param name="fromClause">From clause.</param>
		/// <param name="queryModel">The query model.</param>
		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
		{
			if (this.Configuration.IsFunction(fromClause.ItemType))
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var functionName = this.Configuration.GetFunctionName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new UserDefinedFunction(functionName) { Alias = alias, Schema = schemaName };
			}
			else
			{
				var schemaName = this.Configuration.GetSchemaName(fromClause.ItemType);
				var tableName = this.Configuration.GetTableName(fromClause.ItemType);
				var alias = fromClause.ItemName.Replace("<generated>", "g");
				this.SelectStatement.Source = new Table(tableName) { Alias = alias, Schema = schemaName };
			}
			base.VisitMainFromClause(fromClause, queryModel);
		}

		/// <summary>
		/// Visits the join clause.
		/// </summary>
		/// <param name="joinClause">The join clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
		{
			// TODO: This seems heavy...
			// TODO: And like it's only going to deal with certain types of joins
			var table = (Table)StatementPartCreator.Visit(queryModel, joinClause.InnerSequence, this.Configuration, this.AliasTables);
            table.Alias = joinClause.ItemName.Replace("<generated>", "g");
			var leftColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.OuterKeySelector, this.Configuration, this.AliasTables);
			var rightColumn = (SourceExpression)StatementPartCreator.Visit(queryModel, joinClause.InnerKeySelector, this.Configuration, this.AliasTables);

			if (leftColumn is FieldCollection leftColumnCollection &&
				rightColumn is FieldCollection rightColumnCollection)
			{
				var joinConditions = new ConditionCollection();
				for (var i = 0; i < leftColumnCollection.Count; i++)
				{
					joinConditions.Add(new Condition(leftColumnCollection[i], SqlOperator.Equals, rightColumnCollection[i]));
				}
				this.SelectStatement.SourceJoins.Add(new Join(table, joinConditions) { JoinType = JoinType.Left });
			}
			else
			{
				this.SelectStatement.SourceJoins.Add(new Join(table, leftColumn, rightColumn) { JoinType = JoinType.Left });
			}

			base.VisitJoinClause(joinClause, queryModel, index);
		}

		/// <summary>
		/// Visits the ordering.
		/// </summary>
		/// <param name="ordering">The ordering.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="orderByClause">The order by clause.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">Invalid ordering direction: {ordering.OrderingDirection}</exception>
		public override void VisitOrdering(Ordering ordering, QueryModel queryModel, OrderByClause orderByClause, int index)
		{
			var column = (Column)StatementPartCreator.Visit(queryModel, ordering.Expression, this.Configuration, this.AliasTables);

			switch (ordering.OrderingDirection)
			{
				case OrderingDirection.Asc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Ascending));
					break;
				}
				case OrderingDirection.Desc:
				{
					this.SelectStatement.OrderByFields.Add(new OrderByExpression(column, OrderDirection.Descending));
					break;
				}
				default:
				{
					throw new InvalidOperationException($"Invalid ordering direction: {ordering.OrderingDirection}");
				}
			}

			base.VisitOrdering(ordering, queryModel, orderByClause, index);
		}

		/// <summary>
		/// Visits the result operator.
		/// </summary>
		/// <param name="resultOperator">The result operator.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		/// <exception cref="InvalidOperationException">
		/// can't count multiple fields
		/// or
		/// can't sum multiple or no fields
		/// or
		/// can't min multiple or no fields
		/// or
		/// can't max multiple or no fields
		/// or
		/// can't average multiple or no fields
		/// </exception>
		/// <exception cref="NotSupportedException">
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// or
		/// Currently not supporting methods or variables in the Skip or Take clause.
		/// </exception>
		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
		{
			if (resultOperator is AnyResultOperator)
			{
				this.SelectStatement.IsAny = true;
				this.SelectStatement.IsAggregate = true;
				return;
			}

			if (resultOperator is AllResultOperator allResults)
			{
				this.SelectStatement.IsAll = true;
				this.SelectStatement.IsAggregate = true;
				var predicate = allResults.Predicate;
				if (predicate != null)
				{
					VisitPredicate(predicate, queryModel);
				}
				return;
			}

			if (resultOperator is ContainsResultOperator containsResult)
			{
				this.SelectStatement.IsContains = true;
				this.SelectStatement.IsAggregate = true;
				var item = containsResult.Item;
				if (item != null && item.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.ContainsItem = new ConstantPart(((ConstantExpression)item).Value);
				}
				return;
			}

			if (resultOperator is FirstResultOperator)
			{
				this.SelectStatement.Limit = 1;
				return;
			}

			if (resultOperator is LastResultOperator)
			{
				this.SelectStatement.Limit = 1;
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			if (resultOperator is CountResultOperator || resultOperator is LongCountResultOperator)
			{
				// Throw an exception if there is more than one field
				if (this.SelectStatement.SourceFields.Count > 1)
				{
					throw new InvalidOperationException("can't count multiple fields");
				}

				// Count the first field
				if (this.SelectStatement.SourceFields.Count == 0)
				{
					this.SelectStatement.SourceFields.Add(new Aggregate(AggregateType.Count, new Column("*")));
				}
				else
				{
					this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Count, (Field)this.SelectStatement.SourceFields[0]);
				}

				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is SumResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't sum multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Sum, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MinResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't min multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Min, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is MaxResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't max multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Max, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is AverageResultOperator)
			{
				// Throw an exception if there is not one field
				if (this.SelectStatement.SourceFields.Count != 1)
				{
					throw new InvalidOperationException("can't average multiple or no fields");
				}

				// Sum the first field
				this.SelectStatement.SourceFields[0] = new Aggregate(AggregateType.Average, (Field)this.SelectStatement.SourceFields[0]);
				this.SelectStatement.IsAggregate = true;

				return;
			}

			if (resultOperator is DistinctResultOperator)
			{
				this.SelectStatement.IsDistinct = true;
				return;
			}

			if (resultOperator is TakeResultOperator takeResult)
			{
				var count = takeResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.Limit = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is SkipResultOperator skipResult)
			{
				var count = skipResult.Count;
				if (count.NodeType == ExpressionType.Constant)
				{
					this.SelectStatement.StartIndex = (int)((ConstantExpression)count).Value;
				}
				else
				{
					throw new NotSupportedException("Currently not supporting methods or variables in the Skip or Take clause.");
				}
				return;
			}

			if (resultOperator is ReverseResultOperator)
			{
				foreach (var orderBy in this.SelectStatement.OrderByFields)
				{
					orderBy.Direction = (orderBy.Direction == OrderDirection.Ascending) ? OrderDirection.Descending : OrderDirection.Ascending;
				}
				return;
			}

			base.VisitResultOperator(resultOperator, queryModel, index);
		}

		/// <summary>
		/// Visits the where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		/// <param name="queryModel">The query model.</param>
		/// <param name="index">The index.</param>
		public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
		{
			VisitPredicate(whereClause.Predicate, queryModel);

			base.VisitWhereClause(whereClause, queryModel, index);
		}

		private void VisitPredicate(Expression predicate, QueryModel queryModel)
		{
			var whereStatement = StatementPartCreator.Visit(queryModel, predicate, this.Configuration, this.AliasTables);
			ConditionExpression condition;
			if (whereStatement is ConditionExpression conditionWhere)
			{
				condition = conditionWhere;
			}
			else if (whereStatement is UnaryOperation unaryWhere && unaryWhere.Expression is ConditionExpression unaryWhereExpression)
			{
				condition = unaryWhereExpression;
			}
			else if (whereStatement is UnaryOperation unaryWhere2 && unaryWhere2.Expression is Column)
			{
				var unary = unaryWhere2;
				var column = (Column)unary.Expression;
				condition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
			}
			else if (whereStatement is ConstantPart constantWhere && constantWhere.Value is bool booleanWhere)
			{
				condition = new Condition() {
					Field = new ConstantPart(booleanWhere),
					Operator = SqlOperator.Equals,
					Value = new ConstantPart(true)
				};
			}
			else if (whereStatement is Column columnWhere && columnWhere.PropertyType == typeof(bool))
			{
				condition = new Condition(columnWhere, SqlOperator.Equals, new ConstantPart(true));
			}
			else
			{
				throw new InvalidOperationException();
			}
			this.SelectStatement.Conditions.Add(condition);
		}
	}

	/// <summary>
	/// A dummy implementation of IQueryExecutor for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <seealso cref="IQueryExecutor" />
	internal class StatementExecutor : IQueryExecutor
	{
		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a collection query, i.e. as a query returning objects of type <typeparamref name="T" />.
		/// The query does not end with a scalar result operator, but it can end with a single result operator, for example
		/// <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" /> or <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" />. In such a case, the returned enumerable must yield exactly
		/// one object (or none if the last result operator allows empty result sets).
		/// </summary>
		/// <typeparam name="T">The type of the items returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a scalar query, i.e. as a query returning a scalar value of type <typeparamref name="T" />.
		/// The query ends with a scalar result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.CountResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SumResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the scalar value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <returns>
		/// A scalar value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteScalar<T>(QueryModel queryModel)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Executes the given <paramref name="queryModel" /> as a single object query, i.e. as a query returning a single object of type
		/// <typeparamref name="T" />.
		/// The query ends with a single result operator, for example a <see cref="T:Remotion.Linq.Clauses.ResultOperators.FirstResultOperator" /> or a <see cref="T:Remotion.Linq.Clauses.ResultOperators.SingleResultOperator" />.
		/// </summary>
		/// <typeparam name="T">The type of the single value returned by the query.</typeparam>
		/// <param name="queryModel">The <see cref="T:Remotion.Linq.QueryModel" /> representing the query to be executed. Analyze this via an
		/// <see cref="T:Remotion.Linq.IQueryModelVisitor" />.</param>
		/// <param name="returnDefaultWhenEmpty">If <see langword="true" />, the executor must return a default value when its result set is empty;
		/// if <see langword="false" />, it should throw an <see cref="T:System.InvalidOperationException" /> when its result set is empty.</param>
		/// <returns>
		/// A single value of type <typeparamref name="T" /> that represents the query's result.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		/// <remarks>
		/// The difference between <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> and <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is in the kind of object that is returned.
		/// <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteSingle``1(Remotion.Linq.QueryModel,System.Boolean)" /> is used when a query that would otherwise return a collection result set should pick a single value from the
		/// set, for example the first, last, minimum, maximum, or only value in the set. <see cref="M:Remotion.Linq.IQueryExecutor.ExecuteScalar``1(Remotion.Linq.QueryModel)" /> is used when a value is
		/// calculated or aggregated from all the values in the collection result set. This applies to, for example, item counts, average calculations,
		/// checks for the existence of a specific item, and so on.
		/// </remarks>
		public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
		{
			throw new NotImplementedException();
		}
	}

	/// <summary>
	/// Converts Expressions (such as those in Re-Linq's QueryModels) into StatementParts.
	/// </summary>
	internal class StatementPartCreator : RelinqExpressionVisitor
	{
		private QueryModel QueryModel { get; set; }

		private DatabaseMapper Configuration { get; set; }

		private bool AliasTables { get; set; }

		private Stack<StatementPart> Stack { get; set; }

		private StatementPartCreator(QueryModel queryModel, DatabaseMapper mapper, bool aliasTables)
		{
			this.QueryModel = queryModel;
			this.Configuration = mapper;
			this.AliasTables = aliasTables;
			this.Stack = new Stack<StatementPart>();
		}

		public static StatementPart Visit(QueryModel queryModel, Expression expression, DatabaseMapper mapper, bool aliasTables)
		{
			var visitor = new StatementPartCreator(queryModel, mapper, aliasTables);
			visitor.Visit(expression);
			return visitor.Stack.Pop();
		}

		protected override Expression VisitBinary(BinaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				case ExpressionType.ExclusiveOr:
				{
					if (expression.Type == typeof(bool))
					{
						return VisitBinaryConditionCollection(expression);
					}
					else
					{
						return VisitBinaryOperation(expression);
					}
				}
				case ExpressionType.Equal:
				case ExpressionType.NotEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				{
					return VisitBinaryCondition(expression);
				}
				case ExpressionType.Add:
				case ExpressionType.Subtract:
				case ExpressionType.Multiply:
				case ExpressionType.Divide:
				case ExpressionType.Modulo:
				case ExpressionType.LeftShift:
				case ExpressionType.RightShift:
				{
					return VisitBinaryOperation(expression);
				}
			}

			return base.VisitBinary(expression);
		}

		private Expression VisitBinaryConditionCollection(BinaryExpression expression)
		{
			Visit(expression.Left);
			Visit(expression.Right);

			// Convert the conditions on the stack to a collection and set each condition's relationship
			var newCondition = new ConditionCollection();
			for (var i = 0; i < 2; i++)
			{
				ConditionExpression subCondition;
				if (this.Stack.Peek() is ConditionExpression)
				{
					subCondition = (ConditionExpression)this.Stack.Pop();
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp && unaryOp.Expression is ConditionExpression)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					subCondition = (ConditionExpression)unary.Expression;
				}
				else if (this.Stack.Peek() is UnaryOperation unaryOp2 && unaryOp2.Expression is Column)
				{
					var unary = (UnaryOperation)this.Stack.Pop();
					var column = (Column)unary.Expression;
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(unary.Operator != UnaryOperator.Not));
				}
				else if (this.Stack.Peek() is ConstantPart constantPart && constantPart.Value is bool)
				{
					var constant = (ConstantPart)this.Stack.Pop();
					var value = (bool)constant.Value;
					subCondition = new Condition() {
						Field = new ConstantPart(value),
						Operator = SqlOperator.Equals,
						Value = new ConstantPart(true)
					};
				}
				else if (this.Stack.Peek() is Column columnPart && columnPart.PropertyType == typeof(bool))
				{
					var column = (Column)this.Stack.Pop();
					subCondition = new Condition(column, SqlOperator.Equals, new ConstantPart(true));
				}
				else
				{
					break;
				}

				if (subCondition != null)
				{
					newCondition.Insert(0, subCondition);

					if (expression.NodeType == ExpressionType.And ||
						expression.NodeType == ExpressionType.AndAlso)
					{
						subCondition.Relationship = ConditionRelationship.And;
					}
					else
					{
						subCondition.Relationship = ConditionRelationship.Or;
					}
				}
			}

			if (newCondition.Count > 1)
			{
				this.Stack.Push(newCondition);
			}
			else
			{
				this.Stack.Push(newCondition[0]);
			}

			return expression;
		}

		private Expression VisitBinaryCondition(BinaryExpression expression)
		{
			var newCondition = new Condition();
			Visit(expression.Left);
			newCondition.Field = this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Equal:
				{
					newCondition.Operator = SqlOperator.Equals;
					break;
				}
				case ExpressionType.NotEqual:
				{
					newCondition.Operator = SqlOperator.NotEquals;
					break;
				}
				case ExpressionType.LessThan:
				{
					newCondition.Operator = SqlOperator.IsLessThan;
					break;
				}
				case ExpressionType.LessThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsLessThanOrEqualTo;
					break;
				}
				case ExpressionType.GreaterThan:
				{
					newCondition.Operator = SqlOperator.IsGreaterThan;
					break;
				}
				case ExpressionType.GreaterThanOrEqual:
				{
					newCondition.Operator = SqlOperator.IsGreaterThanOrEqualTo;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newCondition.Value = this.Stack.Pop();

			if (newCondition.Field.PartType == StatementPartType.FieldCollection)
			{
				// If anonymous types have been passed in for multi-value checking, we need to split
				// them out manually from the field collection and constant part that Relinq creates
				var fields = (FieldCollection)newCondition.Field;
				var value = ((ConstantPart)newCondition.Value).Value;
				var valueList = value.GetType().GetProperties().Select(x => x.GetValue(value, null)).ToList();
				var newConditionCollection = new ConditionCollection();
				// Swap the operator if it's NotEquals
				var op = newCondition.Operator;
				if (op == SqlOperator.NotEquals)
				{
					op = SqlOperator.Equals;
					newConditionCollection.Not = true;
				}
				for (var i = 0; i < fields.Count; i++)
				{
					newConditionCollection.Add(new Condition(fields[i], op, valueList[i]));
				}
				this.Stack.Push(newConditionCollection);
			}
			else
			{
				this.Stack.Push(newCondition);
			}

			return expression;
		}

		private Expression VisitBinaryOperation(BinaryExpression expression)
		{
			var newBinary = new BinaryOperation();
			Visit(expression.Left);
			newBinary.Left = (SourceExpression)this.Stack.Pop();

			switch (expression.NodeType)
			{
				case ExpressionType.Add:
				{
					newBinary.Operator = BinaryOperator.Add;
					break;
				}
				case ExpressionType.Subtract:
				{
					newBinary.Operator = BinaryOperator.Subtract;
					break;
				}
				case ExpressionType.Multiply:
				{
					newBinary.Operator = BinaryOperator.Multiply;
					break;
				}
				case ExpressionType.Divide:
				{
					newBinary.Operator = BinaryOperator.Divide;
					break;
				}
				case ExpressionType.Modulo:
				{
					newBinary.Operator = BinaryOperator.Remainder;
					break;
				}
				case ExpressionType.LeftShift:
				{
					newBinary.Operator = BinaryOperator.LeftShift;
					break;
				}
				case ExpressionType.RightShift:
				{
					newBinary.Operator = BinaryOperator.RightShift;
					break;
				}
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				{
					newBinary.Operator = BinaryOperator.BitwiseAnd;
					break;
				}
				case ExpressionType.Or:
				case ExpressionType.OrElse:
				{
					newBinary.Operator = BinaryOperator.BitwiseOr;
					break;
				}
				case ExpressionType.ExclusiveOr:
				{
					newBinary.Operator = BinaryOperator.ExclusiveOr;
					break;
				}
				default:
				{
					// TODO: Throw that as a better exception
					throw new NotSupportedException("Unhandled NodeType: " + expression.NodeType);
				}
			}

			Visit(expression.Right);
			newBinary.Right = (SourceExpression)this.Stack.Pop();
			this.Stack.Push(newBinary);

			return expression;
		}

		protected override Expression VisitConditional(ConditionalExpression node)
		{
			var newConditionalCase = new ConditionalCase();
			Visit(node.Test);
			newConditionalCase.Test = this.Stack.Pop();
			Visit(node.IfTrue);
			newConditionalCase.IfTrue = this.Stack.Pop();
			Visit(node.IfFalse);
			newConditionalCase.IfFalse = this.Stack.Pop();
			this.Stack.Push(newConditionalCase);
			return node;
		}

		protected override Expression VisitConstant(ConstantExpression expression)
		{
			if (expression.Value == null)
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			else if (this.Configuration.ShouldMapType(expression.Type))
			{
				var primaryKeyName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
				var property = expression.Value.GetType().GetProperty(primaryKeyName);
				var value = property.GetValue(expression.Value);
				this.Stack.Push(new ConstantPart(value));
			}
			else if (TypeHelper.IsGenericType(expression.Type, typeof(IQueryable<>)))
			{
				var queryType = expression.Value.GetType().GetGenericArguments()[0];
				var tableName = this.Configuration.GetTableName(queryType);
				this.Stack.Push(new Table(tableName));
			}
			else
			{
				this.Stack.Push(new ConstantPart(expression.Value));
			}
			return expression;
		}

		protected override Expression VisitMember(MemberExpression expression)
		{
			if (expression.Member.DeclaringType == typeof(string))
			{
				switch (expression.Member.Name)
				{
					case "Length":
					{
						var newFunction = new StringLengthFunction();
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.DeclaringType == typeof(DateTime) || expression.Member.DeclaringType == typeof(DateTimeOffset))
			{
				switch (expression.Member.Name)
				{
					case "Date":
					{
						var newFunction = new DatePartFunction(DatePart.Date);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Day":
					{
						var newFunction = new DatePartFunction(DatePart.Day);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Month":
					{
						var newFunction = new DatePartFunction(DatePart.Month);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Year":
					{
						var newFunction = new DatePartFunction(DatePart.Year);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Hour":
					{
						var newFunction = new DatePartFunction(DatePart.Hour);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Minute":
					{
						var newFunction = new DatePartFunction(DatePart.Minute);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Second":
					{
						var newFunction = new DatePartFunction(DatePart.Second);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "Millisecond":
					{
						var newFunction = new DatePartFunction(DatePart.Millisecond);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfWeek":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfWeek);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
					case "DayOfYear":
					{
						var newFunction = new DatePartFunction(DatePart.DayOfYear);
						Visit(expression.Expression);
						newFunction.Argument = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return expression;
					}
				}
			}
			else if (expression.Member.MemberType == MemberTypes.Property)
			{
				string tableName;
				if (this.AliasTables)
				{
					if (expression.Expression is UnaryExpression unaryExpression)
					{
						var source = (QuerySourceReferenceExpression)unaryExpression.Operand;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else if (expression.Expression is MemberExpression memberExpression)
					{
						var source = (QuerySourceReferenceExpression)memberExpression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
					else
					{
						var source = (QuerySourceReferenceExpression)expression.Expression;
						tableName = source.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
					}
				}
				else
				{
					// The property may be declared on a base type, so we can't just get DeclaringType
					// Instead, we get the type from the expression that was used to reference it
					var propertyType = expression.Expression.Type;

					// HACK: Replace interfaces with actual tables
					//	There has to be a way of intercepting the QueryModel creation??
					if (propertyType.IsInterface)
					{
						propertyType = this.QueryModel.MainFromClause.ItemType;
					}

					tableName = this.Configuration.GetTableName(propertyType);
				}

				var property = (PropertyInfo)expression.Member;
				var columnName = this.Configuration.GetColumnName(property);
				if (this.Configuration.IsRelatedItem(property))
				{
					// TODO: Should this be done here, or when converting the statement to SQL?
					columnName = this.Configuration.GetForeignKeyColumnName(property);
				}
				var newColumn = new Column(tableName, columnName) { PropertyType = property.PropertyType };
				this.Stack.Push(newColumn);
				return expression;
			}

			throw new NotSupportedException($"The member access '{expression.Member}' is not supported");
		}

		protected override Expression VisitMethodCall(MethodCallExpression expression)
		{
			var handled = false;

			if (expression.Method.DeclaringType == typeof(string))
			{
				handled = VisitStringMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(DateTime))
			{
				handled = VisitDateTimeMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(decimal))
			{
				handled = VisitDecimalMethodCall(expression);
			}
			else if (expression.Method.DeclaringType == typeof(Math))
			{
				handled = VisitMathMethodCall(expression);
			}

			if (!handled)
			{
				if (expression.Method.Name == "ToString")
				{
					handled = VisitToStringMethodCall(expression);
				}
				else if (expression.Method.Name == "Equals")
				{
					handled = VisitEqualsMethodCall(expression);
				}
				else if (!expression.Method.IsStatic && expression.Method.Name == "CompareTo" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 1)
				{
					handled = VisitCompareToMethodCall(expression);
				}
				else if (expression.Method.IsStatic && expression.Method.Name == "Compare" && expression.Method.ReturnType == typeof(int) && expression.Arguments.Count == 2)
				{
					handled = VisitCompareMethodCall(expression);
				}
			}

			return handled ? expression : base.VisitMethodCall(expression);
		}

		private bool VisitStringMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "StartsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.StartsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "EndsWith":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.EndsWith;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Contains":
				{
					var newCondition = new Condition();
					newCondition.Operator = SqlOperator.Contains;
					this.Visit(expression.Object);
					newCondition.Field = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newCondition.Value = this.Stack.Pop();
					this.Stack.Push(newCondition);
					return true;
				}
				case "Concat":
				{
					var newFunction = new StringConcatenateFunction();
					IList<Expression> args = expression.Arguments;
					if (args.Count == 1 && args[0].NodeType == ExpressionType.NewArrayInit)
					{
						args = ((NewArrayExpression)args[0]).Expressions;
					}
					for (var i = 0; i < args.Count; i++)
					{
						this.Visit(args[i]);
						newFunction.Arguments.Add(this.Stack.Pop());
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IsNullOrEmpty":
				{
					var newCondition = new ConditionCollection();

					var isNullCondition = new Condition();
					this.Visit(expression.Arguments[0]);
					isNullCondition.Field = this.Stack.Pop();
					isNullCondition.Operator = SqlOperator.Equals;
					isNullCondition.Value = new ConstantPart(null);
					newCondition.Add(isNullCondition);

					var notEqualsCondition = new Condition();
					notEqualsCondition.Relationship = ConditionRelationship.Or;
					this.Visit(expression.Arguments[0]);
					notEqualsCondition.Field = this.Stack.Pop();
					notEqualsCondition.Operator = SqlOperator.Equals;
					notEqualsCondition.Value = new ConstantPart("");
					newCondition.Add(notEqualsCondition);

					this.Stack.Push(newCondition);
					return true;
				}
				case "ToUpper":
				case "ToUpperInvariant":
				{
					var newFunction = new StringToUpperFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "ToLower":
				case "ToLowerInvariant":
				{
					var newFunction = new StringToLowerFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Replace":
				{
					var newFunction = new StringReplaceFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.OldValue = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.NewValue = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Substring":
				{
					var newFunction = new SubstringFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Remove":
				{
					var newFunction = new StringRemoveFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StartIndex = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Length = this.Stack.Pop();
					}
					else
					{
						newFunction.Length = new ConstantPart(8000);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "IndexOf":
				{
					var newFunction = new StringIndexFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.StringToFind = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.StartIndex = this.Stack.Pop();
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Trim":
				{
					var newFunction = new StringTrimFunction();
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDateTimeMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "op_Subtract":
				{
					if (expression.Arguments[1].Type == typeof(DateTime))
					{
						var newFunction = new DateDifferenceFunction();
						this.Visit(expression.Arguments[0]);
						newFunction.Date1 = this.Stack.Pop();
						this.Visit(expression.Arguments[1]);
						newFunction.Date2 = this.Stack.Pop();
						this.Stack.Push(newFunction);
						return true;
					}
					break;
				}
				case "AddDays":
				{
					var newFunction = new DateAddFunction(DatePart.Day);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMonths":
				{
					var newFunction = new DateAddFunction(DatePart.Month);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddYears":
				{
					var newFunction = new DateAddFunction(DatePart.Year);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddHours":
				{
					var newFunction = new DateAddFunction(DatePart.Hour);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMinutes":
				{
					var newFunction = new DateAddFunction(DatePart.Minute);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddSeconds":
				{
					var newFunction = new DateAddFunction(DatePart.Second);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "AddMilliseconds":
				{
					var newFunction = new DateAddFunction(DatePart.Millisecond);
					this.Visit(expression.Object);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[0]);
					newFunction.Number = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitDecimalMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Add":
				case "Subtract":
				case "Multiply":
				case "Divide":
				case "Remainder":
				{
					var newOperation = new BinaryOperation();
					this.Visit(expression.Arguments[0]);
					newOperation.Left = (SourceExpression)this.Stack.Pop();
					newOperation.Operator = (BinaryOperator)Enum.Parse(typeof(BinaryOperator), expression.Method.Name);
					this.Visit(expression.Arguments[1]);
					newOperation.Right = (SourceExpression)this.Stack.Pop();
					this.Stack.Push(newOperation);
					return true;
				}
				case "Negate":
				{
					var newFunction = new NumberNegateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Compare":
				{
					this.Visit(Expression.Condition(
						Expression.Equal(expression.Arguments[0], expression.Arguments[1]),
						Expression.Constant(0),
						Expression.Condition(
							Expression.LessThan(expression.Arguments[0], expression.Arguments[1]),
							Expression.Constant(-1),
							Expression.Constant(1)
							)));
					return true;
				}
			}

			return false;
		}

		private bool VisitMathMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Log":
				{
					var newFunction = new NumberLogFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Log10":
				{
					var newFunction = new NumberLog10Function();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sign":
				{
					var newFunction = new NumberSignFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Exp":
				{
					var newFunction = new NumberExponentialFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sqrt":
				{
					var newFunction = new NumberRootFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					newFunction.Root = new ConstantPart(2);
					this.Stack.Push(newFunction);
					return true;
				}
				case "Pow":
				{
					var newFunction = new NumberPowerFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					newFunction.Power = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Abs":
				{
					var newFunction = new NumberAbsoluteFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Ceiling":
				{
					var newFunction = new NumberCeilingFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Floor":
				{
					var newFunction = new NumberFloorFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Round":
				{
					var newFunction = new NumberRoundFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count == 2 && expression.Arguments[1].Type == typeof(int))
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Precision = this.Stack.Pop();
					}
					else
					{
						// TODO: Make it consistent where these are set
						// should they be defaults here, or in the function class, or when making the sql
						// probably when making the sql, because the appropriate default will differ between platforms
						newFunction.Precision = new ConstantPart(0);
					}
					this.Stack.Push(newFunction);
					return true;
				}
				case "Truncate":
				{
					var newFunction = new NumberTruncateFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					this.Stack.Push(newFunction);
					return true;
				}
				case "Sin":
				case "Cos":
				case "Tan":
				case "Acos":
				case "Asin":
				case "Atan":
				case "Atan2":
				{
					var newFunction = new NumberTrigFunction();
					this.Visit(expression.Arguments[0]);
					newFunction.Argument = this.Stack.Pop();
					if (expression.Arguments.Count > 1)
					{
						this.Visit(expression.Arguments[1]);
						newFunction.Argument2 = this.Stack.Pop();
					}
					newFunction.Function = (TrigFunction)Enum.Parse(typeof(TrigFunction), expression.Method.Name);
					this.Stack.Push(newFunction);
					return true;
				}
			}

			return false;
		}

		private bool VisitToStringMethodCall(MethodCallExpression expression)
		{
			if (expression.Object.Type == typeof(string))
			{
				this.Visit(expression.Object);
			}
			else
			{
				var newFunction = new ConvertFunction();
				this.Visit(expression.Arguments[0]);
				newFunction.Expression = (SourceExpression)this.Stack.Pop();
				this.Stack.Push(newFunction);
			}
			return true;
		}

		private bool VisitEqualsMethodCall(MethodCallExpression expression)
		{
			var condition = new Condition();
			condition.Operator = SqlOperator.Equals;
			if (expression.Method.IsStatic && expression.Method.DeclaringType == typeof(object))
			{
				this.Visit(expression.Arguments[0]);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[1]);
				condition.Value = this.Stack.Pop();
			}
			else if (!expression.Method.IsStatic && expression.Arguments.Count > 0 && expression.Arguments[0].Type == expression.Object.Type)
			{
				// TODO: Get the other arguments, most importantly StringComparison
				this.Visit(expression.Object);
				condition.Field = this.Stack.Pop();
				this.Visit(expression.Arguments[0]);
				condition.Value = this.Stack.Pop();
			}
			this.Stack.Push(condition);
			return true;
		}

		private bool VisitCompareToMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Object);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[0]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		private bool VisitCompareMethodCall(MethodCallExpression expression)
		{
			var newFunction = new StringCompareFunction();
			this.Visit(expression.Arguments[0]);
			newFunction.Argument = this.Stack.Pop();
			this.Visit(expression.Arguments[1]);
			newFunction.Other = this.Stack.Pop();
			this.Stack.Push(newFunction);
			return true;
		}

		protected override Expression VisitNew(NewExpression expression)
		{
			if (expression.Type == typeof(DateTime))
			{
				// It's a date, so put its arguments into a DateNewFunction
				var function = new DateNewFunction();
				if (expression.Arguments.Count == 3)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
				}
				else if (expression.Arguments.Count == 6)
				{
					this.Visit(expression.Arguments[0]);
					function.Year = this.Stack.Pop();
					this.Visit(expression.Arguments[1]);
					function.Month = this.Stack.Pop();
					this.Visit(expression.Arguments[2]);
					function.Day = this.Stack.Pop();
					this.Visit(expression.Arguments[3]);
					function.Hour = this.Stack.Pop();
					this.Visit(expression.Arguments[4]);
					function.Minute = this.Stack.Pop();
					this.Visit(expression.Arguments[5]);
					function.Second = this.Stack.Pop();
				}
				this.Stack.Push(function);
				return expression;
			}
			else if (expression.Arguments.Count > 0)
			{
				// It's a new anonymous object, so get its properties as columns
				var fields = new FieldCollection();
				foreach (var argument in expression.Arguments)
				{
					this.Visit(argument);
					fields.Add((SourceExpression)this.Stack.Pop());
				}
				this.Stack.Push(fields);
				return expression;
			}

			return base.VisitNew(expression);
		}

		protected override Expression VisitUnary(UnaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.Not:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Not;
					Visit(expression.Operand);

					newOperation.Expression = this.Stack.Pop();
					// Push the condition onto the stack instead
					if (newOperation.Expression is Condition newCondition)
					{
						newCondition.Not = true;
						this.Stack.Push(newCondition);
					}
					else
					{
						this.Stack.Push(newOperation);
					}
					return expression;
				}
				case ExpressionType.Negate:
				case ExpressionType.NegateChecked:
				{
					var newOperation = new UnaryOperation();
					newOperation.Operator = UnaryOperator.Negate;
					Visit(expression.Operand);
					newOperation.Expression = this.Stack.Pop();
					this.Stack.Push(newOperation);
					return expression;
				}
				case ExpressionType.UnaryPlus:
				{
					Visit(expression.Operand);
					return expression;
				}
				case ExpressionType.Convert:
				{
					// Ignore conversions for now
					Visit(expression.Operand);
					return expression;
				}
				default:
				{
					throw new NotSupportedException($"The unary operator '{expression.NodeType}' is not supported");
				}
			}
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
		{
			var tableName = expression.ReferencedQuerySource.ItemName.Replace("<generated>", "g");
			var columnName = this.Configuration.GetPrimaryKeyColumnName(expression.Type);
			var newColumn = new Column(tableName, columnName);
			this.Stack.Push(newColumn);

			return base.VisitQuerySourceReference(expression);
		}

		protected override Expression VisitSubQuery(SubQueryExpression expression)
		{
			if (expression.QueryModel.ResultOperators.Count > 0 &&
				expression.QueryModel.ResultOperators[0] is Remotion.Linq.Clauses.ResultOperators.ContainsResultOperator contains)
			{
				// It's an Array.Contains, so we need to convert the subquery into a condition
				var newCondition = new Condition();
				newCondition.Operator = SqlOperator.IsIn;

				Visit(contains.Item);
				newCondition.Field = this.Stack.Pop();

				if (TypeHelper.IsGenericType(expression.QueryModel.MainFromClause.FromExpression.Type, typeof(IQueryable<>)))
				{
					// Create the sub-select statement
					var subselect = StatementCreator.Visit(expression.QueryModel, this.Configuration, true);
					subselect.IsContains = false;
					if (subselect.SourceFields.Count == 0)
					{
						var subselectField = expression.QueryModel.SelectClause.Selector;
						Visit(subselectField);
						subselect.SourceFields.Add((SourceExpression)this.Stack.Pop());
					}
					newCondition.Value = subselect;
				}
				else
				{
					// Just check in the array that was passed
					Visit(expression.QueryModel.MainFromClause.FromExpression);
					newCondition.Value = this.Stack.Pop();
				}

				this.Stack.Push(newCondition);
			}

			return base.VisitSubQuery(expression);
		}

#if DEBUG

		// NOTE: I got sick of re-adding these everytime I wanted to figure out what was going on, so
		// I'm leaving them here in debug only

		protected override Expression VisitBlock(BlockExpression node)
		{
			BreakpointHook();
			return base.VisitBlock(node);
		}

		protected override CatchBlock VisitCatchBlock(CatchBlock node)
		{
			BreakpointHook();
			return base.VisitCatchBlock(node);
		}

		protected override Expression VisitDebugInfo(DebugInfoExpression node)
		{
			BreakpointHook();
			return base.VisitDebugInfo(node);
		}

		protected override Expression VisitDefault(DefaultExpression node)
		{
			BreakpointHook();
			return base.VisitDefault(node);
		}

		protected override Expression VisitDynamic(DynamicExpression node)
		{
			BreakpointHook();
			return base.VisitDynamic(node);
		}

		protected override ElementInit VisitElementInit(ElementInit node)
		{
			BreakpointHook();
			return base.VisitElementInit(node);
		}

		protected override Expression VisitExtension(Expression node)
		{
			BreakpointHook();
			return base.VisitExtension(node);
		}

		protected override Expression VisitGoto(GotoExpression node)
		{
			BreakpointHook();
			return base.VisitGoto(node);
		}

		protected override Expression VisitIndex(IndexExpression node)
		{
			BreakpointHook();
			return base.VisitIndex(node);
		}

		protected override Expression VisitInvocation(InvocationExpression node)
		{
			BreakpointHook();
			return base.VisitInvocation(node);
		}

		protected override Expression VisitLabel(LabelExpression node)
		{
			BreakpointHook();
			return base.VisitLabel(node);
		}

		protected override LabelTarget VisitLabelTarget(LabelTarget node)
		{
			BreakpointHook();
			return base.VisitLabelTarget(node);
		}

		protected override Expression VisitLambda<T>(Expression<T> node)
		{
			BreakpointHook();
			return base.VisitLambda(node);
		}

		protected override Expression VisitListInit(ListInitExpression node)
		{
			BreakpointHook();
			return base.VisitListInit(node);
		}

		protected override Expression VisitLoop(LoopExpression node)
		{
			BreakpointHook();
			return base.VisitLoop(node);
		}

		protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
		{
			BreakpointHook();
			return base.VisitMemberAssignment(node);
		}

		protected override MemberBinding VisitMemberBinding(MemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberBinding(node);
		}

		protected override Expression VisitMemberInit(MemberInitExpression node)
		{
			BreakpointHook();
			return base.VisitMemberInit(node);
		}

		protected override MemberListBinding VisitMemberListBinding(MemberListBinding node)
		{
			BreakpointHook();
			return base.VisitMemberListBinding(node);
		}

		protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node)
		{
			BreakpointHook();
			return base.VisitMemberMemberBinding(node);
		}

		protected override Expression VisitNewArray(NewArrayExpression node)
		{
			BreakpointHook();
			return base.VisitNewArray(node);
		}

		protected override Expression VisitParameter(ParameterExpression node)
		{
			BreakpointHook();
			return base.VisitParameter(node);
		}

		protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
		{
			BreakpointHook();
			return base.VisitRuntimeVariables(node);
		}

		protected override Expression VisitSwitch(SwitchExpression node)
		{
			BreakpointHook();
			return base.VisitSwitch(node);
		}

		protected override SwitchCase VisitSwitchCase(SwitchCase node)
		{
			BreakpointHook();
			return base.VisitSwitchCase(node);
		}

		protected override Expression VisitTry(TryExpression node)
		{
			BreakpointHook();
			return base.VisitTry(node);
		}

		protected override Expression VisitTypeBinary(TypeBinaryExpression node)
		{
			BreakpointHook();
			return base.VisitTypeBinary(node);
		}

		// When creating statement parts, put a breakpoint here if you would like to debug
		protected void BreakpointHook()
		{
		}
#endif
	}

	/// <summary>
	/// A dummy implementation of QueryableBase for visiting statement conditions e.g. in Delete.Where.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso cref="Remotion.Linq.QueryableBase{T}" />
	internal class StatementQuery<T> : QueryableBase<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="queryParser">The <see cref="T:Remotion.Linq.Parsing.Structure.IQueryParser" /> used to parse queries. Specify an instance of
		/// <see cref="T:Remotion.Linq.Parsing.Structure.QueryParser" /> for default behavior. See also <see cref="M:Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault" />.</param>
		/// <param name="executor">The <see cref="T:Remotion.Linq.IQueryExecutor" /> used to execute the query represented by this <see cref="T:Remotion.Linq.QueryableBase`1" />.</param>
		public StatementQuery(IQueryParser queryParser, IQueryExecutor executor)
			: base(new DefaultQueryProvider(typeof(StatementQuery<>), queryParser, executor))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="StatementQuery{T}"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
		/// <param name="expression">The expression.</param>
		public StatementQuery(IQueryProvider provider, Expression expression)
			: base(provider, expression)
		{
		}
	}

	/// <summary>
	/// Contains helper methods for dealing with types.
	/// </summary>
	public static class TypeHelper
	{
		///// <summary>
		///// Finds any interfaces of type IEnumerable on a type.
		///// </summary>
		///// <param name="sequenceType">The type to search for IEnumerable.</param>
		///// <returns></returns>
		//public static Type FindIEnumerable(Type sequenceType)
		//{
		//	if (sequenceType == null || sequenceType == typeof(string))
		//	{
		//		return null;
		//	}

		//	if (sequenceType.IsArray)
		//	{
		//		return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
		//	}

		//	if (sequenceType.IsGenericType)
		//	{
		//		foreach (Type arg in sequenceType.GetGenericArguments())
		//		{
		//			Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
		//			if (ienum.IsAssignableFrom(sequenceType))
		//			{
		//				return ienum;
		//			}
		//		}
		//	}

		//	Type[] interfaces = sequenceType.GetInterfaces();
		//	if (interfaces != null && interfaces.Length > 0)
		//	{
		//		foreach (Type iface in interfaces)
		//		{
		//			Type ienum = FindIEnumerable(iface);
		//			if (ienum != null)
		//				return ienum;
		//		}
		//	}

		//	if (sequenceType.BaseType != null && sequenceType.BaseType != typeof(object))
		//	{
		//		return FindIEnumerable(sequenceType.BaseType);
		//	}

		//	return null;
		//}

		///// <summary>
		///// Gets the type of element contained in a sequence.
		///// </summary>
		///// <param name="sequenceType">The type of the sequence, which must implement an IEnumerable interface.</param>
		///// <returns></returns>
		//public static Type GetElementType(Type sequenceType)
		//{
		//	Type enumerableType = FindIEnumerable(sequenceType);
		//	if (enumerableType == null)
		//	{
		//		return sequenceType;
		//	}
		//	else
		//	{
		//		return enumerableType.GetGenericArguments()[0];
		//	}
		//}

		/// <summary>
		/// Determines whether the specified type is nullable.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns><c>true</c> if the specified type is nullable; otherwise, <c>false</c>.</returns>
		public static bool IsNullableType(Type type)
		{
			return type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		/// <summary>
		/// Gets a non-nullable version of the supplied type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public static Type GetNonNullableType(Type type)
		{
			if (IsNullableType(type))
			{
				return type.GetGenericArguments()[0];
			}
			return type;
		}

		///// <summary>
		///// Determines whether the specified type is boolean.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is boolean; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsBoolean(Type type)
		//{
		//	return Type.GetTypeCode(type) == TypeCode.Boolean;
		//}

		//public static bool IsInteger(Type type)
		//{
		//	Type nnType = GetNonNullableType(type);
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.SByte:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.Byte:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		///// <summary>
		///// Determines whether the specified type is numeric.
		///// </summary>
		///// <param name="type">The type.</param>
		///// <returns>
		/////   <c>true</c> if the specified type is numeric; otherwise, <c>false</c>.
		///// </returns>
		//public static bool IsNumeric(Type type)
		//{
		//	switch (Type.GetTypeCode(type))
		//	{
		//		case TypeCode.Byte:
		//		case TypeCode.Decimal:
		//		case TypeCode.Double:
		//		case TypeCode.Int16:
		//		case TypeCode.Int32:
		//		case TypeCode.Int64:
		//		case TypeCode.SByte:
		//		case TypeCode.Single:
		//		case TypeCode.UInt16:
		//		case TypeCode.UInt32:
		//		case TypeCode.UInt64:
		//		{
		//			return true;
		//		}
		//		default:
		//		{
		//			return false;
		//		}
		//	}
		//}

		//public static bool IsAnonymous(Type type)
		//{
		//	// From http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
		//	// HACK: The only way to detect anonymous types right now.
		//	return
		//		Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false) &&
		//		type.IsGenericType &&
		//		type.Name.Contains("AnonymousType") &&
		//		(type.Name.StartsWith("<>") || type.Name.StartsWith("VB$")) &&
		//		(type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
		//}

		public static bool IsGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/982487/testing-if-object-is-of-generic-type-in-c-sharp
			while (type != null)
			{
				if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
				if (genericType.IsInterface && IsAssignableToGenericType(type, genericType))
				{
					return true;
				}
				type = type.BaseType;
			}
			return false;
		}

		public static bool IsAssignableToGenericType(Type type, Type genericType)
		{
			// From http://stackoverflow.com/questions/5461295/using-isassignablefrom-with-generics
			var interfaceTypes = type.GetInterfaces();

			foreach (var it in interfaceTypes)
			{
				if (it.IsGenericType && it.GetGenericTypeDefinition() == genericType)
				{
					return true;
				}
			}

			if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
			{
				return true;
			}

			var baseType = type.BaseType;
			if (baseType == null)
			{
				return false;
			}

			return IsAssignableToGenericType(baseType, genericType);
		}

		///// <summary>
		///// Returns an object of the specified type and whose value is equivalent to the specified object.
		///// </summary>
		///// <param name="value">An object that implements the System.IConvertible interface.</param>
		///// <param name="conversionType">The type of object to return.</param>
		///// <returns>
		///// An object whose type is conversionType and whose value is equivalent to value.-or-A
		///// null reference (Nothing in Visual Basic), if value is null and conversionType
		///// is not a value type.
		///// </returns>
		//public static object ChangeType(object value, Type conversionType)
		//{
		//	if (value == null || value == DBNull.Value)
		//	{
		//		// TODO: Maybe not...
		//		// It would be better to make this generic and pass in the default value
		//		// But that would involve changing emitted code
		//		return null;
		//	}

		//	Type safeType = Nullable.GetUnderlyingType(conversionType) ?? conversionType;
		//	if (safeType.IsEnum)
		//	{
		//		return Enum.ToObject(safeType, value);
		//	}
		//	else
		//	{
		//		return Convert.ChangeType(value, safeType);
		//	}
		//}
	}

	/// <summary>
	/// An aggregate operation (such as sum or count) on a source field.
	/// </summary>
	public sealed class Aggregate : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Aggregate;
			}
		}

		/// <summary>
		/// Gets or sets a value indicating whether this aggregate operation is distinct.
		/// </summary>
		/// <value>
		/// <c>true</c> if this aggregate operation is distinct; otherwise, <c>false</c>.
		/// </value>
		public bool IsDistinct { get; set; }

		/// <summary>
		/// Gets or sets the type of the aggregate (e.g. sum, or count).
		/// </summary>
		/// <value>
		/// The type of the aggregate.
		/// </value>
		public AggregateType AggregateType { get; internal set; }

		/// <summary>
		/// Gets or sets the field to be aggregated.
		/// </summary>
		/// <value>
		/// The field.
		/// </value>
		public Field Field { get; internal set; }

		// TODO: Remove all of the internal empty constructors
		/// <summary>
		/// Initializes a new instance of the <see cref="Aggregate" /> class.
		/// </summary>
		internal Aggregate()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Aggregate" /> class.
		/// </summary>
		/// <param name="aggregateType">The type of the aggregate (e.g. sum, or count).</param>
		/// <param name="field">The field to be aggregated.</param>
		public Aggregate(AggregateType aggregateType, Field field)
		{
			this.AggregateType = aggregateType;
			this.Field = field;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.AggregateType.ToString());
			b.Append("(");
			if (this.IsDistinct)
			{
				b.Append("DISTINCT ");
			}
			if (this.Field != null)
			{
				b.Append(this.Field.ToString());
			}
			else
			{
				b.Append("ALL");
			}
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// An aggregate that may be applied to a field such as sum or count.
	/// </summary>
	public enum AggregateType
	{
		/// <summary>
		/// No aggregate.
		/// </summary>
		None,
		/// <summary>
		/// Counts the number of items.
		/// </summary>
		Count,
		/// <summary>
		/// Counts the number of items and returns a large integer.
		/// </summary>
		BigCount,
		/// <summary>
		/// Adds the values contained in the field together.
		/// </summary>
		Sum,
		/// <summary>
		/// Returns the minimum value contained in the field.
		/// </summary>
		Min,
		/// <summary>
		/// Returns the maximum value contained in the field.
		/// </summary>
		Max,
		/// <summary>
		/// Returns the average value contained in the field.
		/// </summary>
		Average,
	}

	/// <summary>
	/// A class for replacing parameters in an expression.
	/// </summary>
	/// <remarks>
	/// This class is used to consolidate anonymous parameters when combining lambda expressions, so
	/// that all of the parameters have the same object reference.
	/// </remarks>
	internal sealed class AnonymousParameterReplacer : ExpressionVisitor
	{
		private readonly ReadOnlyCollection<ParameterExpression> _parameters;

		/// <summary>
		/// Prevents a default instance of the <see cref="AnonymousParameterReplacer" /> class from being created.
		/// </summary>
		/// <param name="parameters">The parameters.</param>
		private AnonymousParameterReplacer(ReadOnlyCollection<ParameterExpression> parameters)
		{
			_parameters = parameters;
		}

		/// <summary>
		/// Replaces the parameters in an expression with the supplied parameters.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns></returns>
		public static Expression Replace(Expression expression, ReadOnlyCollection<ParameterExpression> parameters)
		{
			return new AnonymousParameterReplacer(parameters).Visit(expression);
		}

		/// <summary>
		/// Visits the <see cref="T:System.Linq.Expressions.ParameterExpression" />.
		/// </summary>
		/// <param name="node">The expression to visit.</param>
		/// <returns>
		/// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
		/// </returns>
		protected override Expression VisitParameter(ParameterExpression node)
		{
			foreach (var parameter in _parameters)
			{
				if (parameter.Type == node.Type)
				{
					return parameter;
				}
			}
			return node;
		}
	}

	/// <summary>
	/// An operation with a binary operator e.g. 1 + 2.
	/// </summary>
	public sealed class BinaryOperation : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.BinaryOperation;
			}
		}

		/// <summary>
		/// Gets or sets the expression on the left of the operator.
		/// </summary>
		/// <value>
		/// The left expression.
		/// </value>
		public SourceExpression Left { get; internal set; }

		/// <summary>
		/// Gets or sets the operator.
		/// </summary>
		/// <value>
		/// The operator.
		/// </value>
		public BinaryOperator Operator { get; internal set; }

		private string OperatorString
		{
			get
			{
				switch (this.Operator)
				{
					case BinaryOperator.Add:
					{
						return "+";
					}
					case BinaryOperator.Subtract:
					{
						return "-";
					}
					case BinaryOperator.Multiply:
					{
						return "*";
					}
					case BinaryOperator.Divide:
					{
						return "/";
					}
					case BinaryOperator.Remainder:
					{
						return "%";
					}
					case BinaryOperator.ExclusiveOr:
					{
						return "^";
					}
					case BinaryOperator.LeftShift:
					{
						return "<<";
					}
					case BinaryOperator.RightShift:
					{
						return ">>";
					}
					case BinaryOperator.BitwiseAnd:
					{
						return "&";
					}
					case BinaryOperator.BitwiseOr:
					{
						return "|";
					}
					case BinaryOperator.BitwiseExclusiveOr:
					{
						return "^";
					}
					default:
					{
						throw new InvalidOperationException("Invalid Operator: " + this.Operator);
					}
				}
			}
		}

		/// <summary>
		/// Gets or sets the expression on the right of the operator.
		/// </summary>
		/// <value>
		/// The right expression.
		/// </value>
		public SourceExpression Right { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="BinaryOperation" /> class.
		/// </summary>
		internal BinaryOperation()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="BinaryOperation" /> class.
		/// </summary>
		/// <param name="left">The expression on the left of the operator.</param>
		/// <param name="op">The operator.</param>
		/// <param name="right">The expression on the right of the operator.</param>
		public BinaryOperation(SourceExpression left, BinaryOperator op, SourceExpression right)
		{
			this.Left = left;
			this.Operator = op;
			this.Right = right;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return "(" + this.Left.ToString() + " " + this.OperatorString + " " + this.Right.ToString() + ")";
		}
	}

	/// <summary>
	/// An operator that is performed on two expressions.
	/// </summary>
	public enum BinaryOperator
	{
		/// <summary>
		/// Add the expressions together.
		/// </summary>
		Add,
		/// <summary>
		/// Subtract the right expression from the left.
		/// </summary>
		Subtract,
		/// <summary>
		/// Multiply the expressions together.
		/// </summary>
		Multiply,
		/// <summary>
		/// Divide the left expression by the right.
		/// </summary>
		Divide,
		/// <summary>
		/// Divide the left expression by the right and return the remainder.
		/// </summary>
		Remainder,
		/// <summary>
		/// Perform an exclusive OR operation on the expressions.
		/// </summary>
		ExclusiveOr,
		/// <summary>
		/// Perform a left shift operation on the expressions.
		/// </summary>
		LeftShift,
		/// <summary>
		/// Perform a right shift operation on the expressions.
		/// </summary>
		RightShift,
		/// <summary>
		/// Perform a bitwise AND operation on the expressions.
		/// </summary>
		BitwiseAnd,
		/// <summary>
		/// Perform a bitwise OR operation on the expressions.
		/// </summary>
		BitwiseOr,
		/// <summary>
		/// Perform a bitwise exclusive OR operation on the expressions.
		/// </summary>
		BitwiseExclusiveOr,
		/// <summary>
		/// Perform a bitwise NOT operation on the expressions.
		/// </summary>
		BitwiseNot,
	}

	/// <summary>
	/// Returns the first non-null expression.
	/// </summary>
	public sealed class CoalesceFunction : Field
	{

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.CoalesceFunction;
			}
		}

		/// <summary>
		/// Gets or sets the first expression.
		/// </summary>
		/// <value>
		/// The first expression.
		/// </value>
		public List<SourceExpression> Arguments { get; } = new List<SourceExpression>();

		/// <summary>
		/// Initializes a new instance of the <see cref="CoalesceFunction" /> class.
		/// </summary>
		internal CoalesceFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CoalesceFunction" /> class.
		/// </summary>
		/// <param name="arguments">The arguments.</param>
		public CoalesceFunction(params SourceExpression[] arguments)
		{
			this.Arguments.AddRange(arguments);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("COALESCE(");
			b.Append(string.Join(", ", this.Arguments.Select(a => a.ToString())));
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// A column in a table.
	/// </summary>
	public sealed class Column : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Column;
			}
		}

		/// <summary>
		/// Gets or sets the table.
		/// </summary>
		/// <value>
		/// The table.
		/// </value>
		public Table Table { get; set; }

		/// <summary>
		/// Gets the name of the column.
		/// </summary>
		/// <value>
		/// The name of the column.
		/// </value>
		public string Name { get; private set; }

		internal Type PropertyType { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Column" /> class.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		public Column(string name)
		{
			this.Name = name;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Column" /> class.
		/// </summary>
		/// <param name="tableName">The name of the table.</param>
		/// <param name="name">The name of the column.</param>
		public Column(string tableName, string name)
		{
			this.Table = new Table(tableName);
			this.Name = name;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Column"/> class.
		/// </summary>
		/// <param name="table">The table.</param>
		/// <param name="name">The name.</param>
		public Column(Table table, string name)
		{
			this.Table = table;
			this.Name = name;
		}
		
		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Table != null)
			{
				b.Append(this.Table.ToString());
				b.Append(".");
			}
			b.Append(this.Name);
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public class Condition : ConditionExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Condition;
			}
		}

		public StatementPart Field { get; set; }

		public SqlOperator Operator { get; set; }

		public StatementPart Value { get; set; }

		internal Condition()
		{
		}

		// TODO: Make these static i.e. public static Condition Where(...) ??
		public Condition(string fieldName, SqlOperator op, object value)
		{
			this.Field = new Column(fieldName);
			this.Operator = op;
			AddValue(value);
		}

		public Condition(string tableName, string fieldName, SqlOperator op, object value)
		{
			this.Field = new Column(tableName, fieldName);
			this.Operator = op;
			AddValue(value);
		}

		public Condition(SourceExpression column, SqlOperator op, object value)
		{
			this.Field = column;
			this.Operator = op;
			AddValue(value);
		}

		public static Condition Where(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value);
		}

		public static Condition Where(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value);
		}

		public static Condition Or(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value) { Relationship = ConditionRelationship.Or };
		}

		public static Condition Or(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value) { Relationship = ConditionRelationship.Or };
		}

		public static Condition And(string fieldName, SqlOperator op, object value)
		{
			return new Condition(fieldName, op, value) { Relationship = ConditionRelationship.And };
		}

		public static Condition And(string tableName, string fieldName, SqlOperator op, object value)
		{
			return new Condition(tableName, fieldName, op, value) { Relationship = ConditionRelationship.And };
		}

		private void AddValue(object value)
		{
			if (value == null)
			{
				this.Value = new ConstantPart(null);
				return;
			}

			//if (value is IEnumerable && !(value is string))
			//{
			//	foreach (object subval in (IEnumerable)value)
			//	{
			//		if (subval is StatementPart)
			//		{
			//			this.Value.Add((StatementPart)subval);
			//		}
			//		else
			//		{
			//			this.Value.Add(new ConstantPart(subval));
			//		}
			//	}
			//}
			//else
			//{
			if (value is StatementPart statementPartValue)
			{
				this.Value = statementPartValue;
			}
			else
			{
				this.Value = new ConstantPart(value);
			}
			//}
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Not)
			{
				b.Append("NOT ");
			}
			b.Append(this.Field.ToString());
			b.Append(" ");
			b.Append(this.Operator.ToString());
			b.Append(" ");
            if (this.Value == null)
            {
                b.Append("NULL");
            }
            else
            {
                b.Append(this.Value.ToString());
            }
			return b.ToString();
		}
	}

	public sealed class ConditionalCase : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionalCase;
			}
		}

		public StatementPart Test { get; set; }

		public StatementPart IfTrue { get; set; }

		public StatementPart IfFalse { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Test is Condition)
			{
				b.Append("(CASE WHEN ");
				b.Append(this.Test.ToString());
				b.Append(" THEN ");
				b.Append(this.IfTrue.ToString());
				var ifFalse = this.IfFalse;
				while (ifFalse is ConditionalCase ifFalseCase)
				{
					b.Append(" WHEN ");
					b.Append(ifFalseCase.Test.ToString());
					b.Append(" THEN ");
					b.Append(ifFalseCase.IfTrue.ToString());
					ifFalse = ifFalseCase.IfFalse;
				}
				b.Append(" ELSE ");
				b.Append(ifFalse.ToString());
				b.Append(")");
			}
			else
			{
				b.Append("(CASE ");
				b.Append(this.Test.ToString());
				b.Append(" WHEN True THEN ");
				b.Append(this.IfTrue.ToString());
				b.Append(" ELSE ");
				b.Append(this.IfFalse.ToString());
				b.Append(")");
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// A collection of conditions.
	/// </summary>
	public sealed class ConditionCollection : ConditionExpression, IEnumerable<ConditionExpression>
	{
		private readonly List<ConditionExpression> _conditions = new List<ConditionExpression>();

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionCollection;
			}
		}

		public int Count
		{
			get
			{
				return _conditions.Count;
			}
		}

		public ConditionExpression this[int index]
		{
			get
			{
				return _conditions[index];
			}
			set
			{
				_conditions[index] = value;
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConditionCollection"/> class.
		/// </summary>
		/// <param name="conditions">The conditions.</param>
		public ConditionCollection(params ConditionExpression[] conditions)
		{
			_conditions.AddRange(conditions);
		}

		public void Add(ConditionExpression item)
		{
			_conditions.Add(item);
		}

		public void Insert(int index, ConditionExpression item)
		{
			_conditions.Insert(index, item);
		}

		public void AddRange(IEnumerable<ConditionExpression> collection)
		{
			_conditions.AddRange(collection);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Not)
			{
				b.Append("Not ");
			}
			if (this.Count > 1)
			{
				b.Append("(");
			}
			for (var i = 0; i < this.Count; i++)
			{
				if (i > 0)
				{
					b.Append(" ");
					b.Append(this[i].Relationship.ToString());
					b.Append(" ");
				}
				b.Append(this[i].ToString());
			}
			if (this.Count > 1)
			{
				b.Append(")");
			}
			return b.ToString();
		}

		public IEnumerator<ConditionExpression> GetEnumerator()
		{
			return _conditions.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _conditions.GetEnumerator();
		}
	}

	/// <summary>
	/// An expression that can be used as a condition.
	/// </summary>
	public abstract class ConditionExpression : StatementPart
	{
		public ConditionRelationship Relationship { get; set; }

		public bool Not { get; set; }
	}

	// TODO: What even is this
	public sealed class ConditionPredicate : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConditionPredicate;
			}
		}

		public StatementPart Predicate { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("(CASE WHEN ");
			b.Append(this.Predicate.ToString());
			b.Append(" THEN True ELSE False)");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// The logical relationship between a set of conditions.
	/// </summary>
	public enum ConditionRelationship
	{
		/// <summary>
		/// The set of conditions should return true if all conditions are true.
		/// </summary>
		And,
		/// <summary>
		/// The set of conditions should return true if any conditions are true.
		/// </summary>
		Or,
	}

	/// <summary>
	/// A statement part containing a constant value.
	/// </summary>
	public sealed class ConstantPart : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConstantPart;
			}
		}

		/// <summary>
		/// Gets the constant value.
		/// </summary>
		/// <value>
		/// The constant value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConstantPart" /> class.
		/// </summary>
		/// <param name="value">The constant value.</param>
		public ConstantPart(object value)
		{
			this.Value = value;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Value == null)
			{
				b.Append("NULL");
			}
			else if (this.Value is string || this.Value is char || this.Value is DateTime)
			{
				b.Append("'");
				b.Append(this.Value.ToString());
				b.Append("'");
			}
			else if (this.Value is IEnumerable enumerable)
			{
				b.Append("{ ");
				var values = new List<string>();
				foreach (var o in enumerable)
				{
					if (o == null)
					{
						values.Add("NULL");
					}
                    else if (o is string || o is char || o is DateTime)
                    {
						values.Add("'" + o.ToString() + "'");
                    }
                    else
                    {
						values.Add(o.ToString());
					}
				}
				b.Append(string.Join(", ", values));
				b.Append(" }");
			}
			else
			{
				b.Append(this.Value.ToString());
			}
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	/// <summary>
	/// Converts an expression to the supplied type.
	/// </summary>
	public sealed class ConvertFunction : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ConvertFunction;
			}
		}

		/// <summary>
		/// Gets or sets the expression to convert.
		/// </summary>
		/// <value>
		/// The expression to convert.
		/// </value>
		public SourceExpression Expression { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConvertFunction" /> class.
		/// </summary>
		internal ConvertFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConvertFunction" /> class.
		/// </summary>
		/// <param name="expression">The expression to convert.</param>
		public ConvertFunction(SourceExpression expression)
		{
			this.Expression = expression;
		}
	}

	public sealed class DateAddFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateAddFunction;
			}
		}

		public DatePart DatePart { get; set; }

		public StatementPart Argument { get; set; }

		public StatementPart Number { get; set; }

		internal DateAddFunction(DatePart datePart)
		{
			this.DatePart = datePart;
		}

		public override string ToString()
		{
			return $"DATEADD({this.DatePart}, {this.Argument}, {this.Number})";
		}
	}

	public sealed class DateDifferenceFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateDifferenceFunction;
			}
		}

		public StatementPart Date1 { get; set; }

		public StatementPart Date2 { get; set; }
	}

	public sealed class DateNewFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DateNewFunction;
			}
		}

		public StatementPart Year { get; set; }

		public StatementPart Month { get; set; }

		public StatementPart Day { get; set; }

		public StatementPart Hour { get; set; }

		public StatementPart Minute { get; set; }

		public StatementPart Second { get; set; }

		public DateNewFunction()
		{
		}

		public override string ToString()
		{
			if (this.Hour != null || this.Minute != null || this.Second != null)
			{
				return "DATENEW(" + this.Year.ToString() + ", " + this.Month.ToString() + ", " + this.Day.ToString() + ", " + this.Hour.ToString() + ", " + this.Minute.ToString() + ", " + this.Second.ToString() + ")";
			}
			else
			{
				return "DATENEW(" + this.Year.ToString() + ", " + this.Month.ToString() + ", " + this.Day.ToString() + ")";
			}
		}
	}

	/// <summary>
	/// A date part.
	/// </summary>
	public enum DatePart
	{
		/// <summary>
		/// The millisecond component of the date's time.
		/// </summary>
		Millisecond,
		/// <summary>
		/// The second component of the date's time.
		/// </summary>
		Second,
		/// <summary>
		/// The minute component of the date's time.
		/// </summary>
		Minute,
		/// <summary>
		/// The hour component of the date's time.
		/// </summary>
		Hour,
		/// <summary>
		/// The day component of the date.
		/// </summary>
		Day,
		/// <summary>
		/// The day of the week component of the date.
		/// </summary>
		DayOfWeek,
		/// <summary>
		/// The day of the year component of the date.
		/// </summary>
		DayOfYear,
		/// <summary>
		/// The month component of the date.
		/// </summary>
		Month,
		/// <summary>
		/// The year component of the date.
		/// </summary>
		Year,
		/// <summary>
		/// The date component of the date.
		/// </summary>
		Date,
	}

	public sealed class DatePartFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.DatePartFunction;
			}
		}

		public DatePart DatePart { get; set; }

		public StatementPart Argument { get; set; }

		internal DatePartFunction(DatePart datePart)
		{
			this.DatePart = datePart;
		}

		public override string ToString()
		{
			return $"DATEPART({this.DatePart}, {this.Argument})";
		}
	}

	public sealed class Exists : ConditionExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Exists;
			}
		}

		public SelectStatement Select { get; set; }

		public override string ToString()
		{
			if (this.Not)
			{
				return "NOT EXISTS " + this.Select.ToString();
			}
			else
			{
				return "EXISTS " + this.Select.ToString();
			}
		}
	}

	internal static class ExpressionExtensions
	{
		public static Expression Equal(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.Equal(expression1, expression2);
		}

		public static Expression NotEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.NotEqual(expression1, expression2);
		}

		public static Expression GreaterThan(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.GreaterThan(expression1, expression2);
		}

		public static Expression GreaterThanOrEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.GreaterThanOrEqual(expression1, expression2);
		}

		public static Expression LessThan(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.LessThan(expression1, expression2);
		}

		public static Expression LessThanOrEqual(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.LessThanOrEqual(expression1, expression2);
		}

		public static Expression And(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.And(expression1, expression2);
		}

		public static Expression AndAlso(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.AndAlso(expression1, expression2);
		}

		public static Expression Or(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.Or(expression1, expression2);
		}

		public static Expression OrElse(this Expression expression1, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.OrElse(expression1, expression2);
		}

		public static Expression Binary(this Expression expression1, ExpressionType op, Expression expression2)
		{
			ConvertExpressions(ref expression1, ref expression2);
			return Expression.MakeBinary(op, expression1, expression2);
		}

		private static void ConvertExpressions(ref Expression expression1, ref Expression expression2)
		{
			if (expression1.Type != expression2.Type)
			{
				var isNullable1 = TypeHelper.IsNullableType(expression1.Type);
				var isNullable2 = TypeHelper.IsNullableType(expression2.Type);
				if (isNullable1 || isNullable2)
				{
					if (TypeHelper.GetNonNullableType(expression1.Type) == TypeHelper.GetNonNullableType(expression2.Type))
					{
						if (!isNullable1)
						{
							expression1 = Expression.Convert(expression1, expression2.Type);
						}
						else if (!isNullable2)
						{
							expression2 = Expression.Convert(expression2, expression1.Type);
						}
					}
				}
			}
		}
	}

	// TODO: I can't remember what the difference is between a Field and a SourceExpression
	//			Figure it out and document it, or combine the two classes
	public abstract class Field : SourceExpression
	{
	}

	/// <summary>
	/// A collection of fields.
	/// </summary>
	public sealed class FieldCollection : SourceExpression, IEnumerable<SourceExpression>
	{
		private readonly List<SourceExpression> _fields = new List<SourceExpression>();

		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.FieldCollection;
			}
		}
		
		public int Count
		{
			get
			{
				return _fields.Count;
			}
		}

		public SourceExpression this[int index]
		{
			get
			{
				return _fields[index];
			}
			set
			{
				_fields[index] = value;
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="FieldCollection"/> class.
		/// </summary>
		/// <param name="fields">The fields.</param>
		public FieldCollection(params Field[] fields)
		{
			_fields.AddRange(fields);
		}

		public void Add(SourceExpression item)
		{
			_fields.Add(item);
		}

		public void Insert(int index, SourceExpression item)
		{
			_fields.Insert(index, item);
		}

		public void AddRange(IEnumerable<SourceExpression> collection)
		{
			_fields.AddRange(collection);
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			if (this.Count > 1)
			{
				b.Append("(");
			}
			for (var i = 0; i < this.Count; i++)
			{
				if (i > 0)
				{
					b.Append(", ");
				}
				b.Append(this[i].ToString());
			}
			if (this.Count > 1)
			{
				b.Append(")");
			}
			return b.ToString();
		}

		public IEnumerator<SourceExpression> GetEnumerator()
		{
			return _fields.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _fields.GetEnumerator();
		}
	}

	public abstract class GenericStatement : Statement
	{
		public abstract Statement CreateStatement(DatabaseMapper mapper);
	}

	public sealed class Join : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Join;
			}
		}

		public JoinType JoinType { get; internal set; }
		
		public StatementPart Table { get; internal set; }

		public ConditionCollection Conditions { get; } = new ConditionCollection();

		internal Join()
		{
		}

		public Join(JoinType joinType, StatementPart right, ConditionExpression condition)
		{
			this.JoinType = joinType;
			this.Table = right;
			this.Conditions.Add(condition);
		}

		public Join(string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			this.JoinType = JoinType.Inner;
			// TODO: Fix this pug fugly syntax
			// TODO: Change field => column in all the SQL stuff?  Column if it's a column, field if it's a statement part
			//this.Left = new Table(leftTableName);
			this.Table = new Table(tableName);
			this.Conditions.Add(new Condition(leftTableName, leftColumnName, SqlOperator.Equals, new Column(rightTableName, rightColumnName)));
		}

		public Join(JoinType joinType, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
		{
			this.JoinType = joinType;
			// TODO: Fix this pug fugly syntax
			// TODO: Change field => column in all the SQL stuff?  Column if it's a column, field if it's a statement part
			//this.Left = new Table(leftTableName);
			this.Table = new Table(tableName);
			this.Conditions.Add(new Condition(leftTableName, leftColumnName, SqlOperator.Equals, new Column(rightTableName, rightColumnName)));
		}

		public Join(Table table, SourceExpression leftColumn, SourceExpression rightColumn)
		{
			this.JoinType = JoinType.Inner;
			this.Table = table;
			this.Conditions.Add(new Condition(leftColumn, SqlOperator.Equals, rightColumn));
		}

		public Join(JoinType joinType, Table table, SourceExpression leftColumn, SourceExpression rightColumn)
		{
			this.JoinType = joinType;
			this.Table = table;
			this.Conditions.Add(new Condition(leftColumn, SqlOperator.Equals, rightColumn));
		}

		public Join(Table table, ConditionCollection conditions)
		{
			this.JoinType = JoinType.Inner;
			this.Table = table;
			this.Conditions.AddRange(conditions);
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.JoinType.ToString());
			b.Append(" JOIN ");
			b.Append(this.Table.ToString());
			if (this.Conditions.Count > 0)
			{
				b.Append(" ON ");
				b.Append(this.Conditions.ToString());
			}
			return b.ToString();
		}
	}

	public enum JoinType
	{
		Inner,
		Left,
		Right,
		Cross,
		CrossApply,
		OuterApply
	}

	public sealed class LiteralPart : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.LiteralPart;
			}
		}

		public string Value { get; private set; }

		public LiteralPart(string value)
		{
			this.Value = value;
		}
	}

	public sealed class NumberAbsoluteFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberAbsoluteFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "ABSOLUTE(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberCeilingFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberCeilingFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "CEILING(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberExponentialFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberExponentialFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "EXPONENTIAL(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberFloorFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberFloorFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "FLOOR(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberLog10Function : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberLog10Function;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LOG10(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberLogFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberLogFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LOG(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberNegateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberNegateFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "NEGATE(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberPowerFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberPowerFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Power { get; set; }

		public override string ToString()
		{
			return "POWER(" + this.Argument.ToString() + ", " + this.Power.ToString() + ")";
		}
	}

	public sealed class NumberRootFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberRootFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Root { get; set; }

		public override string ToString()
		{
			return "ROOT(" + this.Argument.ToString() + ", " + this.Root.ToString() + ")";
		}
	}

	public sealed class NumberRoundFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberRoundFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Precision { get; set; }

		public override string ToString()
		{
			return "ROUND(" + this.Argument.ToString() + ", " + this.Precision + ")";
		}
	}

	public sealed class NumberSignFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberSignFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "SIGN(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberTrigFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberTrigFunction;
			}
		}

		public TrigFunction Function { get; set; }

		public StatementPart Argument { get; set; }

		// For Atan2
		public StatementPart Argument2 { get; set; }

		public override string ToString()
		{
			return this.Function.ToString() + "(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class NumberTruncateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.NumberTruncateFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TRUNCATE(" + this.Argument.ToString() + ")";
		}
	}

	/// <summary>
	/// An expression that is used to order a select statement.
	/// </summary>
	public sealed class OrderByExpression : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.OrderByField;
			}
		}

		/// <summary>
		/// Gets the expression that is ordered by.
		/// </summary>
		/// <value>
		/// The expression.
		/// </value>
		public SourceExpression Expression { get; internal set; }

		/// <summary>
		/// Gets the direction of ordering.
		/// </summary>
		/// <value>
		/// The direction.
		/// </value>
		public OrderDirection Direction { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		internal OrderByExpression()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="expression">The expression that is ordered by.</param>
		public OrderByExpression(SourceExpression expression)
		{
			this.Expression = expression;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="columnName">The name of the column to order by.</param>
		public OrderByExpression(string columnName)
		{
			this.Expression = new Column(columnName);
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="expression">The expression that is ordered by.</param>
		/// <param name="direction">The direction of ordering.</param>
		public OrderByExpression(SourceExpression expression, OrderDirection direction)
		{
			this.Expression = expression;
			this.Direction = direction;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OrderByExpression" /> class.
		/// </summary>
		/// <param name="columnName">The name of the column to order by.</param>
		/// <param name="direction">The direction of ordering.</param>
		public OrderByExpression(string columnName, OrderDirection direction)
		{
			this.Expression = new Column(columnName);
			this.Direction = direction;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			if (this.Direction == OrderDirection.Ascending)
			{
				return this.Expression.ToString();
			}
			else
			{
				return this.Expression.ToString() + " " + this.Direction.ToString();
			}
		}
	}

	/// <summary>
	/// The direction in which an expression is ordered.
	/// </summary>
	public enum OrderDirection
	{
		/// <summary>
		/// The expression is ordered from lowest to highest.
		/// </summary>
		Ascending,
		/// <summary>
		/// The expression is ordered from highest to lowest.
		/// </summary>
		Descending,
	}

	/// <summary>
	/// A parameter for passing to a stored procedure or function.
	/// </summary>
	public class Parameter
	{
		/// <summary>
		/// Gets the name of the parameter.
		/// </summary>
		/// <value>
		/// The name.
		/// </value>
		public string Name { get; private set; }

		/// <summary>
		/// Gets the value of the parameter.
		/// </summary>
		/// <value>
		/// The value.
		/// </value>
		public object Value { get; private set; }

		/// <summary>
		/// Gets the type of the parameter.
		/// </summary>
		/// <value>
		/// The type of the parameter.
		/// </value>
		public Type ParameterType { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Parameter"/> class for use in a query.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="value">The value.</param>
		public Parameter(string name, object value)
		{
			this.Name = name;
			this.Value = value;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Parameter" /> class for use when defining a procedure or function.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="parameterType">Type of the parameter.</param>
		public Parameter(string name, Type parameterType)
		{
			this.Name = name;
			this.ParameterType = parameterType;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name;
		}
	}

	public sealed class RowNumber : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.RowNumber;
			}
		}

		public List<OrderByExpression> OrderByFields { get; } = new List<OrderByExpression>();

		public RowNumber(params OrderByExpression[] orderByFields)
		{
			this.OrderByFields.AddRange(orderByFields);
		}

		public override string ToString()
		{
			return "ROWNUMBER";
		}
	}

	/// <summary>
	/// A field containing a select statement that returns a single value.
	/// </summary>
	public sealed class ScalarField : SourceExpression
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		/// <exception cref="NotImplementedException"></exception>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.ScalarField;
			}
		}

		/// <summary>
		/// Gets the select statement.
		/// </summary>
		/// <value>
		/// The select statement.
		/// </value>
		public SelectStatement Select { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ScalarField"/> class.
		/// </summary>
		internal ScalarField()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ScalarField"/> class.
		/// </summary>
		/// <param name="select">The select statement.</param>
		public ScalarField(SelectStatement select)
		{
			this.Select = select;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append(this.Select.ToString());
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append("s");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public class SelectExpression : SourceExpression
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.SelectExpression;
			}
		}

		public SelectStatement Select { get; set; }

		public SelectExpression(SelectStatement select, string alias = null)
		{
			this.Select = select;
			this.Alias = alias;
		}
	}

	public sealed class SetValue
	{
		public Column Column { get; set; }

		public StatementPart Value { get; set; }

		public SetValue()
		{
		}

		public SetValue(string columnName, object value)
			: this(new Column(columnName), value)
		{
		}

		public SetValue(Column column, object value)
		{
			this.Column = column;
			if (value is StatementPart statementPartValue)
			{
				this.Value = statementPartValue;
			}
			else
			{
				this.Value = new ConstantPart(value);
			}
		}
	}

	/// <summary>
	/// An expression that can be used in the field list of a select statement.
	/// </summary>
	public abstract class SourceExpression : StatementPart
	{
		public string Alias { get; set; }
	}

	public enum SqlOperator
	{
		Equals,
		NotEquals,
		IsLessThan,
		IsLessThanOrEqualTo,
		IsGreaterThan,
		IsGreaterThanOrEqualTo,
		IsIn,
		Contains,
		StartsWith,
		EndsWith
	}

	public abstract class Statement : StatementPart
	{
		public Command Build()
		{
			return Build(new DatabaseMapper(), new SqlCommandBuilder());
		}

		public Command Build(DatabaseMapper mapper)
		{
			return Build(mapper, new SqlCommandBuilder());
		}

		public Command Build(ICommandBuilder builder)
		{
			return Build(new DatabaseMapper(), builder);
		}

		public Command Build(DatabaseMapper mapper, ICommandBuilder builder)
		{
			builder.VisitStatement(this, mapper);
			return new Command(this, builder.CommandText.ToString(), builder.ParameterValues.ToArray());
		}
	}

	/// <summary>
	/// The basic building blocks of SQL statements.
	/// </summary>
	public abstract class StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public abstract StatementPartType PartType { get; }
	}

	public enum StatementPartType
	{
		Select,
		GenericSelect,
		//Insert,
		//GenericInsert,
		//Update,
		//GenericUpdate,
		//Delete,
		//GenericDelete,
		Table,
		Column,
		Join,
		Aggregate,
		Exists,
		// TODO: Rename that!
		ConstantPart,
		OrderByField,
		BinaryOperation,
		UnaryOperation,
		ConditionalCase,
		CoalesceFunction,
		DateDifferenceFunction,
		DateNewFunction,
		DatePartFunction,
		DateAddFunction,
		NumberAbsoluteFunction,
		NumberCeilingFunction,
		NumberFloorFunction,
		NumberNegateFunction,
		NumberRoundFunction,
		NumberTruncateFunction,
		NumberSignFunction,
		NumberPowerFunction,
		NumberRootFunction,
		NumberExponentialFunction,
		NumberLogFunction,
		NumberLog10Function,
		NumberTrigFunction,
		StringIndexFunction,
		StringCompareFunction,
		StringConcatenateFunction,
		StringLengthFunction,
		StringRemoveFunction,
		StringReplaceFunction,
		StringTrimFunction,
		StringToLowerFunction,
		StringToUpperFunction,
		SubstringFunction,
		ConditionPredicate,
		Parameter,
		ConvertFunction,
		ConditionExpression,
		Condition,
		ConditionCollection,
		RowNumber,
		LiteralPart,
		ScalarField,
		FieldCollection,
		SelectExpression,
		UserDefinedFunction
	}

	public sealed class StringCompareFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringCompareFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart Other { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("COMPARE(");
			b.Append(this.Argument.ToString());
			b.Append(", ");
			b.Append(this.Other.ToString());
			b.Append(")");
			return b.ToString();
		}
	}

	public sealed class StringConcatenateFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringConcatenateFunction;
			}
		}

		public List<StatementPart> Arguments { get; } = new List<StatementPart>();

		public override string ToString()
		{
			return "CONCAT(" + string.Join(", ", this.Arguments.Select(a => a.ToString())) + ")";
		}
	}

	public sealed class StringIndexFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringIndexFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StringToFind { get; set; }

		public StatementPart StartIndex { get; set; }

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("INDEXOF(");
			b.Append(this.Argument.ToString());
			b.Append(", ");
			b.Append(this.StringToFind.ToString());
			if (this.StartIndex != null)
			{
				b.Append(", ");
				b.Append(this.StartIndex.ToString());
			}
			b.Append(")");
			return b.ToString();
		}
	}

	public sealed class StringLengthFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringLengthFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "LENGTH(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringRemoveFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringRemoveFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StartIndex { get; set; }

		public StatementPart Length { get; set; }

		public override string ToString()
		{
			return "REMOVE(" + this.Argument.ToString() + ", " + this.StartIndex.ToString() + ", " + this.Length.ToString() + ")";
		}
	}

	public sealed class StringReplaceFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringReplaceFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart OldValue { get; set; }

		public StatementPart NewValue { get; set; }

		public override string ToString()
		{
			return "REPLACE(" + this.Argument.ToString() + ", " + this.OldValue.ToString() + ", " + this.NewValue.ToString() + ")";
		}
	}

	public sealed class StringToLowerFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringToLowerFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TOLOWER(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringToUpperFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringToUpperFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TOUPPER(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class StringTrimFunction : Field
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.StringTrimFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public override string ToString()
		{
			return "TRIM(" + this.Argument.ToString() + ")";
		}
	}

	public sealed class SubstringFunction : StatementPart
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.SubstringFunction;
			}
		}

		public StatementPart Argument { get; set; }

		public StatementPart StartIndex { get; set; }

		public StatementPart Length { get; set; }

		public override string ToString()
		{
			return "SUBSTRING(" + this.Argument.ToString() + ", " + this.StartIndex.ToString() + ", " + this.Length.ToString() + ")";
		}
	}

	/// <summary>
	/// A table in the database.
	/// </summary>
	public sealed class Table : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Table;
			}
		}

		/// <summary>
		/// Gets the name of the schema.
		/// </summary>
		/// <value>
		/// The name of the schema.
		/// </value>
		public string Schema { get; internal set; }

		/// <summary>
		/// Gets the name of the table.
		/// </summary>
		/// <value>
		/// The name of the table.
		/// </value>
		public string Name { get; internal set; }

		/// <summary>
		/// Gets the alias to use for the table.
		/// </summary>
		/// <value>
		/// The alias.
		/// </value>
		public string Alias { get; internal set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Table" /> class.
		/// </summary>
		internal Table()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="Table" /> class.
		/// </summary>
		/// <param name="name">The name of the table.</param>
		/// <param name="alias">The alias to use for the table.</param>
		/// <param name="schema">The schema to use for the table.</param>
		public Table(string name, string alias = null, string schema = null)
		{
			this.Name = name;
			this.Alias = alias;
			this.Schema = schema;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name + (!string.IsNullOrEmpty(this.Alias) ? " AS " + this.Alias : "");
		}
	}

	public sealed class Table<T>
	{
		public Type Type { get; internal set; }

		public string Alias { get; internal set; }

		public Table(Type type)
		{
			this.Type = type;
		}

		public Table(Type type, string alias)
		{
			this.Type = type;
			this.Alias = alias;
		}
	}

	public enum TrigFunction
	{
		Sin,
		Cos,
		Tan,
		Asin,
		Acos,
		Atan,
		Atan2
	}

	/// <summary>
	/// An operation with a single operator e.g. negative 1.
	/// </summary>
	public sealed class UnaryOperation : Field
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.UnaryOperation;
			}
		}

		/// <summary>
		/// Gets or sets the operator.
		/// </summary>
		/// <value>
		/// The operator.
		/// </value>
		public UnaryOperator Operator { get; set; }

		/// <summary>
		/// Gets or sets the expression.
		/// </summary>
		/// <value>
		/// The expression.
		/// </value>
		public StatementPart Expression { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="UnaryOperation" /> class.
		/// </summary>
		internal UnaryOperation()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="UnaryOperation" /> class.
		/// </summary>
		/// <param name="op">The operator.</param>
		/// <param name="expression">The expression.</param>
		public UnaryOperation(UnaryOperator op, StatementPart expression)
		{
			this.Operator = op;
			this.Expression = expression;
		}

		public override string ToString()
		{
			return this.Operator.ToString() + " " + this.Expression.ToString();
		}
	}

	/// <summary>
	/// An operator that is performed on a single expression.
	/// </summary>
	public enum UnaryOperator
	{
		/// <summary>
		/// Makes the expression logically opposite.
		/// </summary>
		Not,
		/// <summary>
		/// Negates the expression.
		/// </summary>
		Negate,
	}

	/// <summary>
	/// A user-defined function in the database.
	/// </summary>
	public sealed class UserDefinedFunction : StatementPart
	{
		/// <summary>
		/// Gets the type of the statement part.
		/// </summary>
		/// <value>
		/// The type of the part.
		/// </value>
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.UserDefinedFunction;
			}
		}

		/// <summary>
		/// Gets the name of the schema.
		/// </summary>
		/// <value>
		/// The name of the schema.
		/// </value>
		public string Schema { get; internal set; }

		/// <summary>
		/// Gets the name of the function.
		/// </summary>
		/// <value>
		/// The name of the function.
		/// </value>
		public string Name { get; internal set; }

		/// <summary>
		/// Gets the alias to use for the function.
		/// </summary>
		/// <value>
		/// The alias.
		/// </value>
		public string Alias { get; internal set; }

		/// <summary>
		/// Gets the paths of related items and collections to include when loading data from this function.
		/// </summary>
		/// <value>
		/// The include paths.
		/// </value>
		public List<Parameter> Parameters { get; } = new List<Parameter>();

		/// <summary>
		/// Initializes a new instance of the <see cref="UserDefinedFunction" /> class.
		/// </summary>
		internal UserDefinedFunction()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="UserDefinedFunction" /> class.
		/// </summary>
		/// <param name="name">The name of the function.</param>
		/// <param name="alias">The alias to use for the function.</param>
		public UserDefinedFunction(string name, string alias = null)
		{
			this.Name = name;
			this.Alias = alias;
		}

		/// <summary>
		/// Returns a <see cref="string" /> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="string" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return this.Name + (!string.IsNullOrEmpty(this.Alias) ? " AS " + this.Alias : "");
		}
	}

	public sealed class SelectStatement : Statement
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.Select;
			}
		}

		public StatementPart Source { get; internal set; }

		public List<Join> SourceJoins { get; } = new List<Join>();

		public List<string> IncludePaths { get; } = new List<string>();

		public List<SourceExpression> SourceFields { get; } = new List<SourceExpression>();

		public List<Table> SourceFieldsFrom { get; } = new List<Table>();

		public bool IsAny { get; set; }

		public bool IsAll { get; set; }

		public bool IsContains { get; set; }

		public StatementPart ContainsItem { get; set; }

		public bool IsDistinct { get; set; }

		public int StartIndex { get; set; }

		public int Limit { get; set; }

		public ConditionCollection Conditions { get; } = new ConditionCollection();

		public List<OrderByExpression> OrderByFields { get; } = new List<OrderByExpression>();

		public List<Column> GroupByFields { get; } = new List<Column>();

		public List<SelectStatement> UnionStatements { get; } = new List<SelectStatement>();

		public string Alias { get; set; }

		public bool IsAggregate { get; set; }

		internal SelectStatement()
		{
		}

		public override string ToString()
		{
			var b = new StringBuilder();
			b.Append("(");
			b.Append("SELECT ");
			if (this.IsAny)
			{
				b.Append("ANY ");
			}
			if (this.IsAll)
			{
				b.Append("ALL ");
			}
			if (this.IsContains)
			{
				b.Append("CONTAINS ");
				b.Append(this.ContainsItem);
				b.Append(" IN ");
			}
			if (this.IsDistinct)
			{
				b.Append("DISTINCT ");
			}
			if (this.Limit == 1)
			{
				b.Append("(Row ");
				b.Append(this.StartIndex);
				b.Append(") ");
			}
			else if (this.StartIndex != 0 || this.Limit != 0)
			{
				b.Append("(Rows ");
				b.Append(this.StartIndex);
				if (this.Limit == 0)
				{
					b.Append("+");
				}
				else
				{
					b.Append("-");
					b.Append(this.StartIndex + this.Limit);
				}
				b.Append(") ");
			}
			if (this.SourceFields.Count > 0)
			{
				b.Append(string.Join(", ", Array.ConvertAll(this.SourceFields.ToArray(), f => f.ToString())));
			}
			else
			{
				b.Append("* ");
			}
			b.AppendLine(" ");
			if (this.Source != null)
			{
				b.Append("FROM ");
				b.Append(this.Source.ToString());
				b.AppendLine(" ");
			}
			// TODO: Do these ever get used?
			if (this.SourceJoins.Count > 0)
			{
				b.Append("JOIN ");
				b.Append(string.Join(" AND ", Array.ConvertAll(this.SourceJoins.ToArray(), j => j.ToString())));
				b.AppendLine(" ");
			}
			if (this.Conditions.Count > 0)
			{
				b.Append("WHERE ");
				b.Append(this.Conditions.ToString());
				b.AppendLine(" ");
			}
			if (this.GroupByFields.Count > 0)
			{
				b.Append("GROUP BY ");
				b.Append(string.Join(", ", Array.ConvertAll(this.GroupByFields.ToArray(), f => f.ToString())));
			}
			if (this.OrderByFields.Count > 0)
			{
				b.Append("ORDER BY ");
				b.Append(string.Join(", ", Array.ConvertAll(this.OrderByFields.ToArray(), f => f.ToString())));
			}
			b.Append(")");
			if (!string.IsNullOrEmpty(this.Alias))
			{
				b.Append(" AS ");
				b.Append(this.Alias);
			}
			return b.ToString();
		}
	}

	public sealed class SelectStatement<T> : GenericStatement
	{
		public override StatementPartType PartType
		{
			get
			{
				return StatementPartType.GenericSelect;
			}
		}

		public Table<T> Source { get; internal set; }

		public List<PropertyInfo> SourceFields { get; } = new List<PropertyInfo>();

		public List<FieldAggregate> AggregateFields { get; } = new List<FieldAggregate>();

		public bool IsAny { get; set; }

		public bool IsAll { get; set; }

		public bool IsDistinct { get; set; }

		public int StartIndex { get; set; }

		public int Limit { get; set; }

		public Expression<Func<T, bool>> Conditions { get; internal set; }

		public List<FieldOrder> OrderByFields { get; internal set; } = new List<FieldOrder>();

		public List<PropertyInfo> GroupByFields { get; internal set; } = new List<PropertyInfo>();

		internal SelectStatement(string alias = null)
		{
			this.Source = new Table<T>(typeof(T), alias);
		}

		public override Statement CreateStatement(DatabaseMapper mapper)
		{
			var select = new SelectStatement();
			select.Source = new Table(mapper.GetTableName(this.Source.Type), this.Source.Alias);
			select.SourceFields.AddRange(this.SourceFields.Select(s => PropertyToSourceField(s, mapper)));
			select.SourceFields.AddRange(this.AggregateFields.Select(s => PropertyToAggregate(s, mapper)));
			select.IsAny = this.IsAny;
			select.IsAll = this.IsAll;
			select.IsDistinct = this.IsDistinct;
			select.StartIndex = this.StartIndex;
			select.Limit = this.Limit;
			if (this.Conditions != null)
			{
				// TODO: Need to handle columns from multiple tables...
				var aliasTables = !string.IsNullOrEmpty(this.Source.Alias);
				foreach (var condition in StatementCreator.VisitStatementConditions(this.Conditions, mapper, aliasTables))
				{
					select.Conditions.Add(condition);
				}
			}
			select.OrderByFields.AddRange(this.OrderByFields.Select(s => PropertyToOrderBy(s, mapper)));
			select.GroupByFields.AddRange(this.GroupByFields.Select(s => PropertyToGroupBy(s, mapper)));
			return select;
		}

		private SourceExpression PropertyToSourceField(PropertyInfo prop, DatabaseMapper mapper)
		{
			if (prop != null)
			{
				return new Column(TableNameOrAlias(mapper, prop.DeclaringType), mapper.GetColumnName(prop));
			}
			else
			{
				return new ConstantPart(null);
			}
		}

		private SourceExpression PropertyToAggregate(FieldAggregate field, DatabaseMapper mapper)
		{
			return new Aggregate(
				field.Aggregate,
				new Column(
					field.Field != null ? TableNameOrAlias(mapper, field.Field.DeclaringType) : "",
					field.Field != null ? mapper.GetColumnName(field.Field) : "*")
				);
		}

		private OrderByExpression PropertyToOrderBy(FieldOrder field, DatabaseMapper mapper)
		{
			return new OrderByExpression(
				new Column(
					TableNameOrAlias(mapper, field.Field.DeclaringType),
					mapper.GetColumnName(field.Field)), field.Direction);
		}

		private Column PropertyToGroupBy(PropertyInfo prop, DatabaseMapper mapper)
		{
			return new Column(
					 TableNameOrAlias(mapper, prop.DeclaringType),
					 mapper.GetColumnName(prop));
		}

		private string TableNameOrAlias(DatabaseMapper mapper, Type t)
		{
			if (t == this.Source.Type && !string.IsNullOrEmpty(this.Source.Alias))
			{
				return this.Source.Alias;
			}
			else
			{
				return mapper.GetTableName(t);
			}
		}
	}
}
