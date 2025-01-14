﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace Watsonia.QueryBuilder
{
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
}
