using System;
using System.Linq;

namespace Watsonia.QueryBuilder
{
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
}
