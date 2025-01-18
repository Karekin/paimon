// Generated from org.apache.spark.sql.catalyst.parser.extensions/PaimonSqlExtensions.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser.extensions;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PaimonSqlExtensionsParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PaimonSqlExtensionsVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(PaimonSqlExtensionsParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCall(PaimonSqlExtensionsParser.CallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPositionalArgument(PaimonSqlExtensionsParser.PositionalArgumentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#callArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedArgument(PaimonSqlExtensionsParser.NamedArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(PaimonSqlExtensionsParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(PaimonSqlExtensionsParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(PaimonSqlExtensionsParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(PaimonSqlExtensionsParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeConstructor(PaimonSqlExtensionsParser.TypeConstructorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#stringMap}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringMap(PaimonSqlExtensionsParser.StringMapContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(PaimonSqlExtensionsParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExponentLiteral(PaimonSqlExtensionsParser.ExponentLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(PaimonSqlExtensionsParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(PaimonSqlExtensionsParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigIntLiteral(PaimonSqlExtensionsParser.BigIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSmallIntLiteral(PaimonSqlExtensionsParser.SmallIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTinyIntLiteral(PaimonSqlExtensionsParser.TinyIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(PaimonSqlExtensionsParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloatLiteral(PaimonSqlExtensionsParser.FloatLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigDecimalLiteral(PaimonSqlExtensionsParser.BigDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifier(PaimonSqlExtensionsParser.MultipartIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(PaimonSqlExtensionsParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link PaimonSqlExtensionsParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(PaimonSqlExtensionsParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(PaimonSqlExtensionsParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link PaimonSqlExtensionsParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(PaimonSqlExtensionsParser.NonReservedContext ctx);
}