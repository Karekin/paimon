// Generated from org.apache.spark.sql.catalyst.parser.extensions/PaimonSqlExtensions.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser.extensions;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PaimonSqlExtensionsParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, CALL=7, TRUE=8, FALSE=9, 
		MAP=10, PLUS=11, MINUS=12, STRING=13, BIGINT_LITERAL=14, SMALLINT_LITERAL=15, 
		TINYINT_LITERAL=16, INTEGER_VALUE=17, EXPONENT_VALUE=18, DECIMAL_VALUE=19, 
		FLOAT_LITERAL=20, DOUBLE_LITERAL=21, BIGDECIMAL_LITERAL=22, IDENTIFIER=23, 
		BACKQUOTED_IDENTIFIER=24, SIMPLE_COMMENT=25, BRACKETED_COMMENT=26, WS=27, 
		UNRECOGNIZED=28;
	public static final int
		RULE_singleStatement = 0, RULE_statement = 1, RULE_callArgument = 2, RULE_expression = 3, 
		RULE_constant = 4, RULE_stringMap = 5, RULE_booleanValue = 6, RULE_number = 7, 
		RULE_multipartIdentifier = 8, RULE_identifier = 9, RULE_quotedIdentifier = 10, 
		RULE_nonReserved = 11;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "statement", "callArgument", "expression", "constant", 
			"stringMap", "booleanValue", "number", "multipartIdentifier", "identifier", 
			"quotedIdentifier", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "','", "')'", "'=>'", "'.'", "'CALL'", "'TRUE'", 
			"'FALSE'", "'MAP'", "'+'", "'-'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "CALL", "TRUE", "FALSE", "MAP", 
			"PLUS", "MINUS", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
			"INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", "FLOAT_LITERAL", 
			"DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
			"SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PaimonSqlExtensions.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PaimonSqlExtensionsParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(PaimonSqlExtensionsParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(24);
			statement();
			setState(28);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(25);
				match(T__0);
				}
				}
				setState(30);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(31);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class CallContext extends StatementContext {
		public TerminalNode CALL() { return getToken(PaimonSqlExtensionsParser.CALL, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public CallContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		int _la;
		try {
			_localctx = new CallContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(33);
			match(CALL);
			setState(34);
			multipartIdentifier();
			setState(35);
			match(T__1);
			setState(44);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << CALL) | (1L << TRUE) | (1L << FALSE) | (1L << MAP) | (1L << MINUS) | (1L << STRING) | (1L << BIGINT_LITERAL) | (1L << SMALLINT_LITERAL) | (1L << TINYINT_LITERAL) | (1L << INTEGER_VALUE) | (1L << EXPONENT_VALUE) | (1L << DECIMAL_VALUE) | (1L << FLOAT_LITERAL) | (1L << DOUBLE_LITERAL) | (1L << BIGDECIMAL_LITERAL) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER))) != 0)) {
				{
				setState(36);
				callArgument();
				setState(41);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(37);
					match(T__2);
					setState(38);
					callArgument();
					}
					}
					setState(43);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(46);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CallArgumentContext extends ParserRuleContext {
		public CallArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callArgument; }
	 
		public CallArgumentContext() { }
		public void copyFrom(CallArgumentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PositionalArgumentContext extends CallArgumentContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterPositionalArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitPositionalArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitPositionalArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NamedArgumentContext extends CallArgumentContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterNamedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitNamedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitNamedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallArgumentContext callArgument() throws RecognitionException {
		CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_callArgument);
		try {
			setState(53);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				_localctx = new PositionalArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(48);
				expression();
				}
				break;
			case 2:
				_localctx = new NamedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(49);
				identifier();
				setState(50);
				match(T__4);
				setState(51);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public StringMapContext stringMap() {
			return getRuleContext(StringMapContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_expression);
		try {
			setState(57);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(55);
				constant();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(56);
				stringMap();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(PaimonSqlExtensionsParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(PaimonSqlExtensionsParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(PaimonSqlExtensionsParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_constant);
		int _la;
		try {
			setState(69);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(59);
				number();
				}
				break;
			case 2:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(60);
				booleanValue();
				}
				break;
			case 3:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(62); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(61);
					match(STRING);
					}
					}
					setState(64); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==STRING );
				}
				break;
			case 4:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(66);
				identifier();
				setState(67);
				match(STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StringMapContext extends ParserRuleContext {
		public TerminalNode MAP() { return getToken(PaimonSqlExtensionsParser.MAP, 0); }
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public StringMapContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stringMap; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterStringMap(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitStringMap(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitStringMap(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringMapContext stringMap() throws RecognitionException {
		StringMapContext _localctx = new StringMapContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_stringMap);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(71);
			match(MAP);
			setState(72);
			match(T__1);
			setState(73);
			constant();
			setState(78);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(74);
				match(T__2);
				setState(75);
				constant();
				}
				}
				setState(80);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(81);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(PaimonSqlExtensionsParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(PaimonSqlExtensionsParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(83);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(PaimonSqlExtensionsParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(PaimonSqlExtensionsParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(PaimonSqlExtensionsParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(PaimonSqlExtensionsParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExponentLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(PaimonSqlExtensionsParser.EXPONENT_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public ExponentLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterExponentLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitExponentLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitExponentLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(PaimonSqlExtensionsParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(PaimonSqlExtensionsParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FloatLiteralContext extends NumberContext {
		public TerminalNode FLOAT_LITERAL() { return getToken(PaimonSqlExtensionsParser.FLOAT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public FloatLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterFloatLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitFloatLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitFloatLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(PaimonSqlExtensionsParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(PaimonSqlExtensionsParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_number);
		int _la;
		try {
			setState(121);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
			case 1:
				_localctx = new ExponentLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(86);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(85);
					match(MINUS);
					}
				}

				setState(88);
				match(EXPONENT_VALUE);
				}
				break;
			case 2:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(90);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(89);
					match(MINUS);
					}
				}

				setState(92);
				match(DECIMAL_VALUE);
				}
				break;
			case 3:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(94);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(93);
					match(MINUS);
					}
				}

				setState(96);
				match(INTEGER_VALUE);
				}
				break;
			case 4:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(98);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(97);
					match(MINUS);
					}
				}

				setState(100);
				match(BIGINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(102);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(101);
					match(MINUS);
					}
				}

				setState(104);
				match(SMALLINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(106);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(105);
					match(MINUS);
					}
				}

				setState(108);
				match(TINYINT_LITERAL);
				}
				break;
			case 7:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(110);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(109);
					match(MINUS);
					}
				}

				setState(112);
				match(DOUBLE_LITERAL);
				}
				break;
			case 8:
				_localctx = new FloatLiteralContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(114);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(113);
					match(MINUS);
					}
				}

				setState(116);
				match(FLOAT_LITERAL);
				}
				break;
			case 9:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(118);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(117);
					match(MINUS);
					}
				}

				setState(120);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultipartIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> parts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
		MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_multipartIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			((MultipartIdentifierContext)_localctx).identifier = identifier();
			((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
			setState(128);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__5) {
				{
				{
				setState(124);
				match(T__5);
				setState(125);
				((MultipartIdentifierContext)_localctx).identifier = identifier();
				((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
				}
				}
				setState(130);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends IdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(PaimonSqlExtensionsParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_identifier);
		try {
			setState(134);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(131);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(132);
				quotedIdentifier();
				}
				break;
			case CALL:
			case TRUE:
			case FALSE:
			case MAP:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(133);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(PaimonSqlExtensionsParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(136);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode CALL() { return getToken(PaimonSqlExtensionsParser.CALL, 0); }
		public TerminalNode TRUE() { return getToken(PaimonSqlExtensionsParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(PaimonSqlExtensionsParser.FALSE, 0); }
		public TerminalNode MAP() { return getToken(PaimonSqlExtensionsParser.MAP, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PaimonSqlExtensionsListener ) ((PaimonSqlExtensionsListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PaimonSqlExtensionsVisitor ) return ((PaimonSqlExtensionsVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(138);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << CALL) | (1L << TRUE) | (1L << FALSE) | (1L << MAP))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\36\u008f\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\3\2\3\2\7\2\35\n\2\f\2\16\2 \13\2\3\2\3\2\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\7\3*\n\3\f\3\16\3-\13\3\5\3/\n\3\3\3\3\3\3\4\3\4\3"+
		"\4\3\4\3\4\5\48\n\4\3\5\3\5\5\5<\n\5\3\6\3\6\3\6\6\6A\n\6\r\6\16\6B\3"+
		"\6\3\6\3\6\5\6H\n\6\3\7\3\7\3\7\3\7\3\7\7\7O\n\7\f\7\16\7R\13\7\3\7\3"+
		"\7\3\b\3\b\3\t\5\tY\n\t\3\t\3\t\5\t]\n\t\3\t\3\t\5\ta\n\t\3\t\3\t\5\t"+
		"e\n\t\3\t\3\t\5\ti\n\t\3\t\3\t\5\tm\n\t\3\t\3\t\5\tq\n\t\3\t\3\t\5\tu"+
		"\n\t\3\t\3\t\5\ty\n\t\3\t\5\t|\n\t\3\n\3\n\3\n\7\n\u0081\n\n\f\n\16\n"+
		"\u0084\13\n\3\13\3\13\3\13\5\13\u0089\n\13\3\f\3\f\3\r\3\r\3\r\2\2\16"+
		"\2\4\6\b\n\f\16\20\22\24\26\30\2\4\3\2\n\13\3\2\t\f\2\u00a0\2\32\3\2\2"+
		"\2\4#\3\2\2\2\6\67\3\2\2\2\b;\3\2\2\2\nG\3\2\2\2\fI\3\2\2\2\16U\3\2\2"+
		"\2\20{\3\2\2\2\22}\3\2\2\2\24\u0088\3\2\2\2\26\u008a\3\2\2\2\30\u008c"+
		"\3\2\2\2\32\36\5\4\3\2\33\35\7\3\2\2\34\33\3\2\2\2\35 \3\2\2\2\36\34\3"+
		"\2\2\2\36\37\3\2\2\2\37!\3\2\2\2 \36\3\2\2\2!\"\7\2\2\3\"\3\3\2\2\2#$"+
		"\7\t\2\2$%\5\22\n\2%.\7\4\2\2&+\5\6\4\2\'(\7\5\2\2(*\5\6\4\2)\'\3\2\2"+
		"\2*-\3\2\2\2+)\3\2\2\2+,\3\2\2\2,/\3\2\2\2-+\3\2\2\2.&\3\2\2\2./\3\2\2"+
		"\2/\60\3\2\2\2\60\61\7\6\2\2\61\5\3\2\2\2\628\5\b\5\2\63\64\5\24\13\2"+
		"\64\65\7\7\2\2\65\66\5\b\5\2\668\3\2\2\2\67\62\3\2\2\2\67\63\3\2\2\28"+
		"\7\3\2\2\29<\5\n\6\2:<\5\f\7\2;9\3\2\2\2;:\3\2\2\2<\t\3\2\2\2=H\5\20\t"+
		"\2>H\5\16\b\2?A\7\17\2\2@?\3\2\2\2AB\3\2\2\2B@\3\2\2\2BC\3\2\2\2CH\3\2"+
		"\2\2DE\5\24\13\2EF\7\17\2\2FH\3\2\2\2G=\3\2\2\2G>\3\2\2\2G@\3\2\2\2GD"+
		"\3\2\2\2H\13\3\2\2\2IJ\7\f\2\2JK\7\4\2\2KP\5\n\6\2LM\7\5\2\2MO\5\n\6\2"+
		"NL\3\2\2\2OR\3\2\2\2PN\3\2\2\2PQ\3\2\2\2QS\3\2\2\2RP\3\2\2\2ST\7\6\2\2"+
		"T\r\3\2\2\2UV\t\2\2\2V\17\3\2\2\2WY\7\16\2\2XW\3\2\2\2XY\3\2\2\2YZ\3\2"+
		"\2\2Z|\7\24\2\2[]\7\16\2\2\\[\3\2\2\2\\]\3\2\2\2]^\3\2\2\2^|\7\25\2\2"+
		"_a\7\16\2\2`_\3\2\2\2`a\3\2\2\2ab\3\2\2\2b|\7\23\2\2ce\7\16\2\2dc\3\2"+
		"\2\2de\3\2\2\2ef\3\2\2\2f|\7\20\2\2gi\7\16\2\2hg\3\2\2\2hi\3\2\2\2ij\3"+
		"\2\2\2j|\7\21\2\2km\7\16\2\2lk\3\2\2\2lm\3\2\2\2mn\3\2\2\2n|\7\22\2\2"+
		"oq\7\16\2\2po\3\2\2\2pq\3\2\2\2qr\3\2\2\2r|\7\27\2\2su\7\16\2\2ts\3\2"+
		"\2\2tu\3\2\2\2uv\3\2\2\2v|\7\26\2\2wy\7\16\2\2xw\3\2\2\2xy\3\2\2\2yz\3"+
		"\2\2\2z|\7\30\2\2{X\3\2\2\2{\\\3\2\2\2{`\3\2\2\2{d\3\2\2\2{h\3\2\2\2{"+
		"l\3\2\2\2{p\3\2\2\2{t\3\2\2\2{x\3\2\2\2|\21\3\2\2\2}\u0082\5\24\13\2~"+
		"\177\7\b\2\2\177\u0081\5\24\13\2\u0080~\3\2\2\2\u0081\u0084\3\2\2\2\u0082"+
		"\u0080\3\2\2\2\u0082\u0083\3\2\2\2\u0083\23\3\2\2\2\u0084\u0082\3\2\2"+
		"\2\u0085\u0089\7\31\2\2\u0086\u0089\5\26\f\2\u0087\u0089\5\30\r\2\u0088"+
		"\u0085\3\2\2\2\u0088\u0086\3\2\2\2\u0088\u0087\3\2\2\2\u0089\25\3\2\2"+
		"\2\u008a\u008b\7\32\2\2\u008b\27\3\2\2\2\u008c\u008d\t\3\2\2\u008d\31"+
		"\3\2\2\2\26\36+.\67;BGPX\\`dhlptx{\u0082\u0088";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}