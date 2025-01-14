﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Watsonia.QueryBuilder
{
	/// <summary>
	/// Builds command text and parameters from a statement for use in an SQL database.
	/// </summary>
	/// <seealso cref="Watsonia.QueryBuilder.ICommandBuilder" />
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
}
