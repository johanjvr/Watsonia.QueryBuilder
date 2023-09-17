using System;
using System.Linq;
using System.Text;

namespace Watsonia.QueryBuilder
{
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
}
