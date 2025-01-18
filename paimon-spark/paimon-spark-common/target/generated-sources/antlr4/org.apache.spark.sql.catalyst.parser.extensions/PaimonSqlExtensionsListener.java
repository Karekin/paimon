// Generated from org.apache.spark.sql.catalyst.parser.extensions/PaimonSqlExtensions.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser.extensions;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PaimonSqlExtensionsParser}.
 */
public interface PaimonSqlExtensionsListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(PaimonSqlExtensionsParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(PaimonSqlExtensionsParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code call}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCall(PaimonSqlExtensionsParser.CallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code call}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCall(PaimonSqlExtensionsParser.CallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 */
	void enterPositionalArgument(PaimonSqlExtensionsParser.PositionalArgumentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 */
	void exitPositionalArgument(PaimonSqlExtensionsParser.PositionalArgumentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 */
	void enterNamedArgument(PaimonSqlExtensionsParser.NamedArgumentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 */
	void exitNamedArgument(PaimonSqlExtensionsParser.NamedArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(PaimonSqlExtensionsParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(PaimonSqlExtensionsParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(PaimonSqlExtensionsParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(PaimonSqlExtensionsParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(PaimonSqlExtensionsParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(PaimonSqlExtensionsParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(PaimonSqlExtensionsParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(PaimonSqlExtensionsParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterTypeConstructor(PaimonSqlExtensionsParser.TypeConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitTypeConstructor(PaimonSqlExtensionsParser.TypeConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#stringMap}.
	 * @param ctx the parse tree
	 */
	void enterStringMap(PaimonSqlExtensionsParser.StringMapContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#stringMap}.
	 * @param ctx the parse tree
	 */
	void exitStringMap(PaimonSqlExtensionsParser.StringMapContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(PaimonSqlExtensionsParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(PaimonSqlExtensionsParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterExponentLiteral(PaimonSqlExtensionsParser.ExponentLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitExponentLiteral(PaimonSqlExtensionsParser.ExponentLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(PaimonSqlExtensionsParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(PaimonSqlExtensionsParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(PaimonSqlExtensionsParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(PaimonSqlExtensionsParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigIntLiteral(PaimonSqlExtensionsParser.BigIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigIntLiteral(PaimonSqlExtensionsParser.BigIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterSmallIntLiteral(PaimonSqlExtensionsParser.SmallIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitSmallIntLiteral(PaimonSqlExtensionsParser.SmallIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterTinyIntLiteral(PaimonSqlExtensionsParser.TinyIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitTinyIntLiteral(PaimonSqlExtensionsParser.TinyIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(PaimonSqlExtensionsParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(PaimonSqlExtensionsParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterFloatLiteral(PaimonSqlExtensionsParser.FloatLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitFloatLiteral(PaimonSqlExtensionsParser.FloatLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigDecimalLiteral(PaimonSqlExtensionsParser.BigDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigDecimalLiteral(PaimonSqlExtensionsParser.BigDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifier(PaimonSqlExtensionsParser.MultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifier(PaimonSqlExtensionsParser.MultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(PaimonSqlExtensionsParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(PaimonSqlExtensionsParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(PaimonSqlExtensionsParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(PaimonSqlExtensionsParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(PaimonSqlExtensionsParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(PaimonSqlExtensionsParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link PaimonSqlExtensionsParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(PaimonSqlExtensionsParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link PaimonSqlExtensionsParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(PaimonSqlExtensionsParser.NonReservedContext ctx);
}