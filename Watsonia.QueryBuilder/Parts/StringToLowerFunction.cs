using System;
using System.Linq;

namespace Watsonia.QueryBuilder
{
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
}
