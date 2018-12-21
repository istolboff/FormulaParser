using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static FormulaParser.Tests.Maybe;
using static FormulaParser.Tests.Either;
using static FormulaParser.Tests.CollectionExtensions;
using static FormulaParser.Tests.ParserExtensions;

namespace FormulaParser.Tests
{
    public sealed class Formula
    {
        public Formula(ValueCalculator valueCalculator, ParameterExpression formulaParameter)
        {
            _delegate = Expression.Lambda(valueCalculator.CalculateExpression, formulaParameter).Compile();
            _expressionText = valueCalculator.ToString();
        }

        public object Apply(object parameter = null)
        {
            return _delegate.DynamicInvoke(parameter);
        }

        public override string ToString()
        {
            return _expressionText;
        }

        private readonly Delegate _delegate;
        private readonly string _expressionText;
    }

    public class FormulaBuilder
    {
        public FormulaBuilder(
            Type targetType, 
            TryParse<Type> tryGetPropertyTypeByName, 
            TryParse<MethodInfo> tryGetFunctionByName)
        {
            _formulaParameter = Expression.Parameter(targetType);

            var integerLiteral = NumericLiteral(new TryParse<int>(int.TryParse));
            var decimalLiteral = NumericLiteral(new TryParse<decimal>(decimal.TryParse));
            var dateTimeLiteral = QuotedLiteral(new TryParse<DateTime>(DateTime.TryParse));
            var stringLiteral = TextInput.QuotedLiteral.Select(ValueCalculator.Constant);
            var literal = dateTimeLiteral.Or(stringLiteral).Or(integerLiteral).Or(decimalLiteral);

            var quotedPropertyName = from openingBracket in TextInput.Lexem("[")
                                     from nameParts in Repeat(TextInput.Identifier, atLeastOnce: true)
                                     from closingBraket in TextInput.Lexem("]")
                                     select string.Join(string.Empty, nameParts);
            var propertyName = quotedPropertyName.Or(TextInput.Identifier);
            var propertyAccessor = from name in propertyName
                                   from property in name.Try(tryGetPropertyTypeByName, n => $"Unknown property: '{n}'").StopParsingIfFailed()
                                   select new ValueCalculator(property, Expression.Property(_formulaParameter, name));

            var parameterList = from openingBrace in TextInput.Lexem("(")
                                from arguments in Optional(UniformList(_formulaTextParser, TextInput.Lexem(",")))
                                from closingBrace in TextInput.Lexem(")")
                                select arguments.Map(a => a.Elements).OrElse(Enumerable.Empty<ValueCalculator>());

            TryParse<Maybe<MethodInfo>> tryGetFunctionOrConditionExpressionByName = (string name, out Maybe<MethodInfo> result) =>
                {
                    result = tryGetFunctionByName(name, out var methodInfo) ? Some(methodInfo) : None;
                    return result.Map(_ => true).OrElse(name == "Iif" || name == "If");
                };

            var functionCall = from functionName in TextInput.Identifier
                               from parameters in parameterList
                               from methodInfo in functionName.Try(tryGetFunctionOrConditionExpressionByName, n => $"Unknown function: {n}").StopParsingIfFailed()
                               from arguments in methodInfo
                                                    .Map(mi => MakeFunctionCallArguments(mi, parameters.ToArray()))
                                                    .OrElse(() => MakeIifFunctionCallArguments(parameters.ToArray()))
                                                    .StopParsingIfFailed()
                               select new ValueCalculator(
                                   methodInfo.Map(mi => mi.ReturnType).OrElse(() => arguments[1].Type),
                                   methodInfo.Map(mi => (Expression)Expression.Call(mi, arguments))
                                             .OrElse(() => Expression.Condition(arguments[0], arguments[1], arguments[2])));

            var bracedExpression = from openingBrace in TextInput.Lexem("(")
                                   from internalExpression in _formulaTextParser
                                   from closingBrace in TextInput.Lexem(")")
                                   select internalExpression;

            var multiplier = from optionalSign in Optional(TextInput.Lexem("-", "+"))
                             from valueCalculator in literal.Or(bracedExpression).Or(functionCall).Or(propertyAccessor)
                             from adjustedCalculator in AsParser(valueCalculator.TryGiveSign(optionalSign.OrElse(string.Empty)))
                             select adjustedCalculator;
            var addend = ArithmeticExpressionParser(multiplier, "*", "/", "%");
            var comparableExpression = ArithmeticExpressionParser(addend, "+", "-");
            var equatableExpression = ArithmeticExpressionParser(comparableExpression, "<", "<=", ">", ">=");
            var bitwiseAndableExpression = ArithmeticExpressionParser(equatableExpression, "==", "!=");
            var bitwiseXorableExpression = ArithmeticExpressionParser(bitwiseAndableExpression, "&");
            var bitwiseOrableExpression = ArithmeticExpressionParser(bitwiseXorableExpression, "^");
            var logicalAndableExpression = ArithmeticExpressionParser(bitwiseOrableExpression, "|");
            var logicalOrableExpression = ArithmeticExpressionParser(logicalAndableExpression, "&&");
            _formulaTextParser = ArithmeticExpressionParser(logicalOrableExpression, "||");
        }

        public Formula Build(string formulaText)
        {
            var parsingResult = EatTrailingSpaces(_formulaTextParser)(new TextInput(formulaText));
            return parsingResult
                    .GetCompleteOrElse(
                        valueCalculator => new Formula(valueCalculator, _formulaParameter), 
                        parsingErrors => throw parsingErrors.AsException(formulaText));
        }

        private Parser<Expression[]> MakeFunctionCallArguments(
            MethodInfo methodInfo, 
            IReadOnlyCollection<ValueCalculator> parameterCalculators)
        {
            var parameters = methodInfo.GetParameters();
            if (parameters.Length != parameterCalculators.Count)
            {
                return Failure<Expression[]>(textInput =>
                    textInput.MakeErrors($"Function {methodInfo.Name} expects {parameters.Length} parameters, {parameterCalculators.Count} provided."));
            }

            var parametersConvertedToCorrectTypes = parameters
                .Zip(parameterCalculators, (parameter, parameterCaclulator) =>
                    new { parameter, valueCaclulator = parameterCaclulator.TryCastTo(parameter.ParameterType) })
                .ToArray();

            var conversionErrors = parametersConvertedToCorrectTypes
                .Select(item => item.valueCaclulator.Fold<ParsingError?>(
                    error => error.Amend(message => $"Argument of a wrong type was used for parameter '{item.parameter.Name}' of the function {methodInfo.Name}() : " + message),
                    _ => null))
                .Where(error => error != null)
                .Select(error => error.Value)
                .ToArray();

            if (conversionErrors.Any())
            {
                return Failure<Expression[]>(textInput => 
                    conversionErrors
                        .Skip(1)
                        .Aggregate(
                            textInput.MakeErrors(conversionErrors.First(), null),
                            (errors, error) => textInput.MakeErrors(error, errors)));
            }

            return Success(
                parametersConvertedToCorrectTypes
                    .Select(item => item.valueCaclulator.Fold(
                        _ => throw new InvalidOperationException("Program logic error: by this time we should have made sure that all parameters are compatible by types."),
                        calculator => calculator.CalculateExpression))
                    .ToArray());
        }

        private static Parser<Expression[]> MakeIifFunctionCallArguments(ValueCalculator[] parameterCalculators)
        {
            if (3 != parameterCalculators.Length)
            {
                return Failure<Expression[]>(textInput =>
                    textInput.MakeErrors($"Function Iif expects 3 parameters, {parameterCalculators.Length} provided."));
            }

            if (parameterCalculators[0].ResultType != typeof(bool))
            {
                return Failure<Expression[]>(textInput =>
                    textInput.MakeErrors($"The first argument of Iif function should be 'System.Boolean', '{parameterCalculators[0].ResultType.Name}' provided."));
            }

            var realArgumentCalculators =
                from resultingType in ValueCalculator.TryToDeduceResultingType("Iif", parameterCalculators[1].ResultType, parameterCalculators[2].ResultType)
                from firstValue in parameterCalculators[1].TryCastTo(resultingType)
                from secondValue in parameterCalculators[2].TryCastTo(resultingType)
                select new[] { parameterCalculators[0].CalculateExpression, firstValue.CalculateExpression, secondValue.CalculateExpression };

            return realArgumentCalculators
                        .Fold(
                            error => Failure<Expression[]>(textInput => textInput.MakeErrors(error, null)),
                            Success);
        }

        private static Parser<ValueCalculator> ArithmeticExpressionParser(
            Parser<ValueCalculator> elementParser, 
            params string[] operationLexems)
        {
            return from elements in UniformList(elementParser, TextInput.Operator(operationLexems))
                   from combinedCalculator in FoldBinaryOperatorsList(elements).StopParsingIfFailed()
                   select combinedCalculator;
        }

        private static Parser<ValueCalculator> FoldBinaryOperatorsList(
            ParsedUniformList<ValueCalculator, string> operands)
        {
            return operands.TailPairs
                    .Aggregate(
                        Right<ParsingError, ValueCalculator>(operands.FirstElement), 
                        (valueCalculatorOrError, pair) =>
                            valueCalculatorOrError.FlatMap(calculator => calculator.TryToApplyBinaryOperator(pair.Link, pair.Right)))
                    .Fold(
                        error => Failure<ValueCalculator>(textInput => textInput.MakeErrors(error, null)), 
                        Success);
        }

        private static Parser<ValueCalculator> NumericLiteral<TNumeric>(TryParse<TNumeric> tryParse) where TNumeric : struct
        {
            return from token in TextInput.RegularToken
                   from numeric in token.Try(tryParse, t => $"Could not parse {typeof(TNumeric).Name} from value {t}")
                   select ValueCalculator.Constant(numeric);
        }

        private static Parser<ValueCalculator> QuotedLiteral<T>(TryParse<T> tryParse)
        {
            return from literal in TextInput.QuotedLiteral
                   from parsedLiteral in literal.Try(tryParse, t => $"Could not parse {typeof(T).Name} from value '{t}'")
                   select ValueCalculator.Constant(parsedLiteral);
        }

        private static Parser<TResult> EatTrailingSpaces<TResult>(Parser<TResult> parser)
        {
            return from result in parser
                   from trailingSpaces in new Parser<int>(TextInput.EatWhiteSpaces)
                   select result;
        }

        private readonly Parser<ValueCalculator> _formulaTextParser;
        private readonly ParameterExpression _formulaParameter;
    }

    public readonly struct ValueCalculator
    {
        public ValueCalculator(Type resultType, Expression calculateExpression)
        {
            ResultType = resultType;
            CalculateExpression = calculateExpression;
        }

        public readonly Type ResultType;

        public readonly Expression CalculateExpression;

        public Either<ParsingError, ValueCalculator> TryCastTo(Type type)
        {
            try
            {
                return Right(
                    ResultType == type
                        ? this
                        : new ValueCalculator(type, Expression.Convert(CalculateExpression, type)));
            }
            catch (InvalidOperationException exception)
            {
                return Left(new ParsingError(exception.Message));
            }
        }

        public Either<ParsingError, ValueCalculator> TryToApplyBinaryOperator(string operatorChars, ValueCalculator secondArgument)
        {
            if ((operatorChars == "&&" || operatorChars == "||") && 
                (ResultType != typeof(bool) || secondArgument.ResultType != typeof(bool)))
            {
                return Left(new ParsingError($"Operator '{operatorChars}' cannot be applied to operands of type '{ResultType.Name}' and '{secondArgument.ResultType}'"));
            }

            var op = KnownBinaryOperations[operatorChars];

            if (ResultType == secondArgument.ResultType)
            {
                return Right(
                    new ValueCalculator(
                        op.ResultIsBoolean ? typeof(bool) : ResultType,
                        op.Expression(CalculateExpression, secondArgument.CalculateExpression)));
            }

            var localThis = this;
            return
                from resultType in TryToDeduceResultingType(operatorChars, ResultType, secondArgument.ResultType)
                from leftOperand in localThis.TryCastTo(resultType)
                from rightOperand in secondArgument.TryCastTo(resultType)
                select new ValueCalculator(
                        op.ResultIsBoolean ? typeof(bool) : resultType, 
                        op.Expression(leftOperand.CalculateExpression, rightOperand.CalculateExpression));
        }

        public override string ToString()
        {
            return $"'{ResultType.Name}': {CalculateExpression}";
        }

        public static ValueCalculator Constant<T>(T value)
        {
            return new ValueCalculator(typeof(T), Expression.Constant(value, typeof(T)));
        }

        public static Either<ParsingError, Type> TryToDeduceResultingType(string operatorChars, Type type1, Type type2)
        {
            var i1 = Array.IndexOf(TypesConversionSequence, type1);
            var i2 = Array.IndexOf(TypesConversionSequence, type2);
            if (i1 < 0 || i2 < 0)
            {
                return Left(new ParsingError($"Operator '{operatorChars}' cannot be applied to operands of type '{type1.Name}' and '{type2.Name}'"));
            }

            return Right(i1 > i2 ? type1 : type2);
        }

        public Either<ParsingError, ValueCalculator> TryGiveSign(string sign)
        {
            if (!IsNumeric && !string.IsNullOrEmpty(sign))
            {
                return Left(new ParsingError($"Operator '{sign}' cannot be applied to operand of type '{ResultType.Name}'"));
            }

            return Right(
                sign != "-" 
                    ? this 
                    : new ValueCalculator(
                        ResultType, 
                        Expression.Multiply(
                            Expression.Constant(Convert.ChangeType(-1, ResultType), ResultType), 
                            CalculateExpression)));
        }

        private bool IsNumeric => TypesConversionSequence.Contains(ResultType);

        private static readonly IReadOnlyDictionary<string, (Func<Expression, Expression, Expression> Expression, bool ResultIsBoolean)> KnownBinaryOperations = 
            new Dictionary<string, (Func<Expression, Expression, Expression>, bool)>
            {
                { "<=", (Expression.LessThanOrEqual, true) },
                { ">=", (Expression.GreaterThanOrEqual, true) },
                { "==", (Expression.Equal, true) },
                { "!=", (Expression.NotEqual, true) },
                { "&&", (Expression.And, true) },
                { "||", (Expression.Or, true) },
                { "+", (Expression.Add, false) },
                { "-", (Expression.Subtract, false) },
                { "*", (Expression.Multiply, false) },
                { "/", (Expression.Divide, false) },
                { "%", (Expression.Modulo, false) },
                { "<", (Expression.LessThan, true) },
                { ">", (Expression.GreaterThan, true) },
                { "&", (Expression.And, false) },
                { "|", (Expression.ExclusiveOr, false) }
            };

        private static readonly Type[] TypesConversionSequence = new[] { typeof(short), typeof(int), typeof(long), typeof(decimal) };
    }

    public readonly struct ParsingError
    {
        public ParsingError(string errorMessage)
        {
            _errorMessage = errorMessage;
        }

        public ParsingError Amend(Func<string, string> amendMessage)
        {
            return new ParsingError(amendMessage(_errorMessage));
        }

        public override string ToString() => _errorMessage;

        private readonly string _errorMessage;
    }

    public sealed class ParsingErrors 
    {
        public ParsingErrors(string errorMessage, TextInput errorLocation)
            : this(new ParsingError(errorMessage), null, errorLocation)
        {
        }

        public ParsingErrors(ParsingError headError, ParsingErrors tailErrors, TextInput errorLocation, bool isFatal = false)
        {
            _headError = headError;
            _tailErrors = tailErrors;
            _errorLocation = errorLocation;
            IsFatal = isFatal;
            Trace.WriteLine("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  " + headError + " " + errorLocation);
        }

        public bool IsFatal { get; }

        public Exception AsException(string formulaText)
        {
            return new InvalidOperationException($"[{formulaText}] parsing errors: {Environment.NewLine}{ToString()}");
        }

        public ParsingErrors MakeFatal()
        {
            return new ParsingErrors(_headError, _tailErrors, _errorLocation, true);
        }

        public override string ToString()
        {
            return string.Join(Environment.NewLine, ListErrors(_tailErrors, _headError)) + " at " + _errorLocation.ToString();
        }

        private static IEnumerable<ParsingError> ListErrors(ParsingErrors errors, ParsingError error)
        {
            var result = new List<ParsingError> { error };
            for (var current = errors; current != null; current = current._tailErrors)
            {
                result.Add(current._headError);
            }

            result.Reverse();
            return result;
        }

        private readonly ParsingError _headError;
        private readonly ParsingErrors _tailErrors;
        private readonly TextInput _errorLocation;
    }

    public readonly struct TextInput
    {
        public TextInput(string text, int currentPosition = 0)
        {
            _text = text ?? string.Empty;
            _currentPosition = currentPosition;
        }

        public bool IsEmpty => _currentPosition == _text.Length;

        public static readonly Parser<string> RegularToken =
            from leadingWhitespaces in Repeat(char.IsWhiteSpace)
            from tokenCharacters in Repeat(ch => !char.IsWhiteSpace(ch) && !OperatorStartingCharacters.Contains(ch) && ch != '\'' && ch != '\\', atLeastOnce: true)
            select string.Join(string.Empty, tokenCharacters);

        public static readonly Parser<string> QuotedLiteral =
            from leadingWhitespaces in Repeat(char.IsWhiteSpace)
            from openingQuote in Match("'")
            from literalCharacters in Repeat((prevCh, ch) => ch != '\'' || prevCh == '\\', atLeastOnce: false)
            from closingQuote in Match("'")
            select string.Join(string.Empty, literalCharacters).Replace("\\", string.Empty);

        public static readonly Parser<string> Identifier =
            from leadingWhitespaces in Repeat(char.IsWhiteSpace)
            from identifierCharcters in Repeat((prevCh, ch) => prevCh == null ? char.IsLetter(ch) : char.IsLetterOrDigit(ch), atLeastOnce: true)
            select identifierCharcters;

        public static Parser<string> Lexem(params string[] lexemsText)
        {
            return from leadingWhitespaces in Repeat(char.IsWhiteSpace)
                   from text in Match(lexemsText)
                   select text;
        }

        public static Parser<string> Operator(string[] operatorLexems)
        {
            var operators = operatorLexems
                .OrderByDescending(op => op.Length)
                .Aggregate(
                    default(Parser<string>),
                    (parser, operatorString) => parser != null ? parser.Or(Match(operatorString)) : Match(operatorString));

            return from leadingWhitespaces in Repeat(char.IsWhiteSpace)
                   from op in operators
                   select op;
        }

        private static Parser<string> Match(params string[] texts)
        {
            return textInput =>
            {
                var matchingText = Array.FindIndex(
                    texts,
                    text => string.Compare(textInput._text, textInput._currentPosition, text, 0, text.Length, StringComparison.Ordinal) == 0);
                return matchingText >= 0
                    ? new ParsingResult<string>(texts[matchingText], textInput.SkipTo(textInput._currentPosition + texts[matchingText].Length))
                    : new ParsingResult<string>(textInput.MakeErrors($"{(texts.Length > 1 ? "One of " : string.Empty)}'{(texts.Length > 1 ? string.Join(" ", texts) : texts.Single())}' expected"));
            };
        }

        private static Parser<string> Repeat(Func<char?, char, bool> isExpectedChar, bool atLeastOnce)
        {
            return textInput =>
            {
                var indexOfFirstUnexpectedChar = textInput._currentPosition;
                for (;
                    indexOfFirstUnexpectedChar < textInput._text.Length &&
                    isExpectedChar(
                        indexOfFirstUnexpectedChar == textInput._currentPosition ? (char?)null : textInput._text[indexOfFirstUnexpectedChar - 1],
                        textInput._text[indexOfFirstUnexpectedChar]);
                    ++indexOfFirstUnexpectedChar)
                {
                }

                return indexOfFirstUnexpectedChar == textInput._currentPosition && atLeastOnce
                    ? new ParsingResult<string>(textInput.MakeErrors("unexpected char"))
                    : new ParsingResult<string>(
                        textInput._text.Substring(textInput._currentPosition, indexOfFirstUnexpectedChar - textInput._currentPosition),
                        textInput.SkipTo(indexOfFirstUnexpectedChar));
            };
        }

        private static Parser<string> Repeat(Func<char, bool> isExpectedChar, bool atLeastOnce = false)
        {
            return Repeat((_, ch) => isExpectedChar(ch), atLeastOnce);
        }

        public ParsingErrors AsUnparsedInputError() =>
            MakeErrors($"There stil remains unparsed text '{_text.Substring(_currentPosition)}' at the end.");

        public ParsingErrors MakeErrors(string errorMessage)
        {
            return new ParsingErrors(errorMessage, this);
        }

        public ParsingErrors MakeErrors(ParsingError headError, ParsingErrors tailErrors)
        {
            return new ParsingErrors(headError, tailErrors, this);
        }

        public override string ToString()
        {
            return _text.Insert(_currentPosition, "^");
        }

        public static ParsingResult<int> EatWhiteSpaces(TextInput input)
        {
            var firstNonSpaceCharPos = input.SkipLeadingSpaces();
            var whitespaceCount = (firstNonSpaceCharPos ?? input._text.Length) - input._currentPosition;
            return new ParsingResult<int>(whitespaceCount, input.SkipTo(firstNonSpaceCharPos ?? input._text.Length));
        }

        private int? SkipLeadingSpaces()
        {
            for (var i = _currentPosition; i < _text.Length; ++i)
            {
                if (!char.IsWhiteSpace(_text[i]))
                {
                    return i;
                }
            }

            return null;
        }

        private TextInput SkipTo(int newPosition)
        {
            Assert.IsTrue(_currentPosition <= newPosition, "SkipTo goes back.");
            Assert.IsTrue(newPosition <= _text.Length, "SkipTo goes beyond end of input.");
            return new TextInput(_text, newPosition);
        }

        private readonly string _text;
        private readonly int _currentPosition;

        private static readonly string[] OperatorLexems = new[] { "<=", ">=", "==", "!=", "&&", "||", "<", ">", ",", "(", ")", "[", "]", "+", "-", "*", "/", "%", "|", "&" };
        private static readonly char[] OperatorStartingCharacters = OperatorLexems.Select(op => op.First()).Distinct().ToArray();
    }

    public readonly struct ParsingResult<TResult>
    {
        public ParsingResult(TResult parsedValue, TextInput remainingInput)
            : this(Right((parsedValue, remainingInput)))
        {
        }

        public ParsingResult(ParsingErrors errors)
            : this(Left(errors))
        {
        }

        private ParsingResult(Either<ParsingErrors, (TResult ParsedValue, TextInput RemainingInput)> result)
        {
            _result = result;
        }

        public ParsingResult<TMappedResult> Map<TMappedResult>(Func<TResult, TMappedResult> mapValue)
        {
            return new ParsingResult<TMappedResult>(
                _result.Map(parsingResult => (mapValue(parsingResult.ParsedValue), parsingResult.RemainingInput)));
        }

        public ParsingResult<TValue2> SelectMany<TIntermediate, TValue2>(
            Func<TResult, Parser<TIntermediate>> selector,
            Func<TResult, TIntermediate, TValue2> projector)
        {
            return new ParsingResult<TValue2>(
                from thisResult in _result
                from intermediateResult in selector(thisResult.ParsedValue)(thisResult.RemainingInput)._result
                select (projector(thisResult.ParsedValue, intermediateResult.ParsedValue), intermediateResult.RemainingInput));
        }

        public TFinalResult GetCompleteOrElse<TFinalResult>(
            Func<TResult, TFinalResult> mapFinalResult,
            Func<ParsingErrors, TFinalResult> processErrors)
        {
            return _result.Fold(
                processErrors,
                parsingResult => parsingResult.RemainingInput.IsEmpty
                                    ? mapFinalResult(parsingResult.ParsedValue)
                                    : processErrors(parsingResult.RemainingInput.AsUnparsedInputError()));
        }

        public ParsingResult<TResult> OrElse(Func<ParsingErrors, ParsingResult<TResult>> getDefaultValue)
        {
            return _result.Fold(
                error => error.IsFatal ? new ParsingResult<TResult>(error) : getDefaultValue(error),
                result => new ParsingResult<TResult>(result.ParsedValue, result.RemainingInput));
        }

        public override string ToString()
        {
            return _result.ToString();
        }

        private readonly Either<ParsingErrors, (TResult ParsedValue, TextInput RemainingInput)> _result;
    }

    public readonly struct ParsedUniformList<TElement, TLink>
    {
        public ParsedUniformList(
            TElement firstElement,
            IReadOnlyCollection<(TLink Link, TElement Right)> tailPairs)
        {
            FirstElement = firstElement;
            TailPairs = tailPairs;
        }

        public readonly TElement FirstElement;

        public readonly IReadOnlyCollection<(TLink Link, TElement Right)> TailPairs;

        public IEnumerable<TElement> Elements => 
            new[] { FirstElement }.Concat(TailPairs.Select(pair => pair.Right));
    }

    public delegate ParsingResult<TResult> Parser<TResult>(TextInput input);

    public delegate bool TryParse<T>(string s, out T result);

    public static class ParserExtensions
    {
        public static Parser<TResult> Success<TResult>(TResult result)
        {
            return textInput => new ParsingResult<TResult>(result, textInput);
        }

        public static Parser<TResult> Failure<TResult>(Func<TextInput, ParsingErrors> makeErrors)
        {
            return textInput => new ParsingResult<TResult>(makeErrors(textInput));
        }

        public static Parser<TResult> StopParsingIfFailed<TResult>(this Parser<TResult> @this)
        {
            return textInput => @this(textInput).OrElse(error => new ParsingResult<TResult>(error.MakeFatal()));
        }

        public static Parser<TValue2> Select<TValue, TValue2>(
            this Parser<TValue> @this,
            Func<TValue, TValue2> selector)
        {
            return textInput => @this(textInput).Map(selector);
        }

        public static Parser<TValue2> SelectMany<TValue, TIntermediate, TValue2>(
            this Parser<TValue> @this,
            Func<TValue, Parser<TIntermediate>> selector,
            Func<TValue, TIntermediate, TValue2> projector)
        {
            return textInput => @this(textInput).SelectMany(selector, projector);
        }

        public static Parser<TResult> Try<TResult>(
            this string @this,
            TryParse<TResult> tryParse,
            Func<string, string> makeErrorMessage)
        {
            return tryParse(@this, out var r) ? Success(r) : Failure<TResult>(textInput => textInput.MakeErrors(makeErrorMessage(@this)));
        }

        public static Parser<Maybe<TResult>> Optional<TResult>(Parser<TResult> @this)
        {
            return textInput =>
            {
                return @this(textInput)
                        .Map(Some)
                        .OrElse(_ => new ParsingResult<Maybe<TResult>>(None, textInput));
            };
        }

        public static Parser<TResult> Or<TResult>(this Parser<TResult> first, Parser<TResult> second)
        {
            return textInput =>
            {
                return first(textInput).OrElse(_ => second(textInput));
            };
        }

        public static Parser<IReadOnlyCollection<TResult>> Repeat<TResult>(Parser<TResult> parser, bool atLeastOnce = false)
        {
            var result = from headElement in parser
                         from tailElements in Repeat(parser)
                         select Concatenate(headElement, tailElements);
            return atLeastOnce ? result : result.Or(Success((IReadOnlyCollection<TResult>)new List<TResult>()));
        }

        public static Parser<ParsedUniformList<TElement, TLink>> UniformList<TElement, TLink>(
            Parser<TElement> elementParser,
            Parser<TLink> linkParser)
        {
            var tailPair = from link in linkParser
                           from element in elementParser
                           select (link, element);

            return from headElement in elementParser
                   from tailElements in Repeat(tailPair)
                   select new ParsedUniformList<TElement, TLink>(headElement, tailElements);
        }

        public static Parser<TResult> AsParser<TResult>(Either<ParsingError, TResult> resultOrError)
        {
            return resultOrError.Fold(
                error => Failure<TResult>(textInput=> textInput.MakeErrors(error, null)), 
                Success);
        }
    }
}