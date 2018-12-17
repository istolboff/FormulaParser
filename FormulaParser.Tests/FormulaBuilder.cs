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

            var integerLiteral = NumericLiteral(new TryParse<int>(int.TryParse), (sign, v) => sign * v);
            var decimalLiteral = NumericLiteral(new TryParse<decimal>(decimal.TryParse), (sign, v) => sign * v);
            var dateTimeLiteral = QuotedLiteral(new TryParse<DateTime>(DateTime.TryParse), '\'');
            var stringLiteral = QuotedLiteral(new TryParse<string>(CopyString), '\'');
            var literal = dateTimeLiteral.Or(stringLiteral).Or(integerLiteral).Or(decimalLiteral);

            var propertyAccessor = from propertyName in QuotedLiteral('[', ']').Or(TextInput.Identifier)
                                   from propertyType in AsParser(tryGetPropertyTypeByName, propertyName, n => $"Unknown property: {n}")
                                   select new ValueCalculator(propertyType, Expression.Property(_formulaParameter, propertyName));

            var parameterList = from openingBrace in OperatorLexem("(")
                                from arguments in Optional(UniformList(_formulaTextParser, OperatorLexem(",")))
                                from closingBrace in OperatorLexem(")")
                                select arguments.Map(a => a.Elements).OrElse(Enumerable.Empty<ValueCalculator>());

            TryParse<Maybe<MethodInfo>> tryGetFunctionOrConditionExpressionByName = (string name, out Maybe<MethodInfo> result) =>
                {
                    result = tryGetFunctionByName(name, out var methodInfo) ? Some(methodInfo) : None;
                    return result.Map(_ => true).OrElse(name == "Iif");
                };

            var functionCall = from functionName in new Parser<string>(TextInput.Identifier)
                               from parameters in parameterList
                               from methodInfo in AsParser(tryGetFunctionOrConditionExpressionByName, functionName, n => $"Unknown function: {n}").StopParsingIfFailed()
                               from arguments in methodInfo
                                                    .Map(mi => MakeFunctionCallArguments(mi, parameters.ToArray()))
                                                    .OrElse(() => MakeIifFunctionCallArguments(parameters.ToArray()))
                                                    .StopParsingIfFailed()
                               select new ValueCalculator(
                                   methodInfo.Map(mi => mi.ReturnType).OrElse(() => arguments[1].Type),
                                   methodInfo.Map(mi => (Expression)Expression.Call(mi, arguments))
                                             .OrElse(() => Expression.Condition(arguments[0], arguments[1], arguments[2])));

            var bracedExpression = from openingBrace in OperatorLexem("(")
                                   from internalExpression in _formulaTextParser
                                   from closingBrace in OperatorLexem(")")
                                   select internalExpression;

            var multiplier = literal.Or(bracedExpression).Or(functionCall).Or(propertyAccessor);
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
                            arguments => Success(arguments));
        }

        private static Parser<ValueCalculator> ArithmeticExpressionParser(
            Parser<ValueCalculator> elementParser, 
            params string[] operationLexems)
        {
            return from elements in UniformList(elementParser, OperatorLexem(operationLexems))
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

        private static Parser<ValueCalculator> NumericLiteral<TNumeric>(
            TryParse<TNumeric> tryParse, 
            Func<int, TNumeric, TNumeric> giveSign) where TNumeric : struct
        {
            return  from headingSpaces in new Parser<int>(TextInput.EatWhiteSpaces)
                    from optionalSign in Optional(OperatorLexem("+", "-"))
                    from numericValue in Literal(tryParse)
                    let literal = giveSign(optionalSign.OrElse("+") == "+" ? 1 : -1, numericValue)
                    select new ValueCalculator(typeof(TNumeric), Expression.Constant(literal, typeof(TNumeric)));
        }

        private static Parser<string> QuotedLiteral(char openingQuoteChar, char? closingQuoteChar = null)
        {
            return textInput => textInput.NextQuotedToken(openingQuoteChar, closingQuoteChar ?? openingQuoteChar);
        }

        private static Parser<ValueCalculator> QuotedLiteral<T>(TryParse<T> tryParse, char openingQuoteChar, char? closingQuoteChar = null)
        {
            return from tokenText in QuotedLiteral(openingQuoteChar, closingQuoteChar)
                   from parsedLiteral in AsParser(
                                            tryParse,
                                            tokenText,
                                            t => $"Could not parse {typeof(T).Name} from value " +
                                                  $"{openingQuoteChar}{t.Replace(openingQuoteChar.ToString(), "\\" + openingQuoteChar)}{openingQuoteChar}")
                   select new ValueCalculator(typeof(T), Expression.Constant(parsedLiteral, typeof(T)));
        }

        private static Parser<T> Literal<T>(TryParse<T> tryParse) where T : struct
        {
            return textInput =>
            {
                return textInput
                        .NextToken()
                        .FlatMap(tokenText => 
                            tryParse(tokenText, out T result)
                                ? Right<ParsingErrors, T>(result)
                                : Left<ParsingErrors, T>(
                                    textInput.MakeErrors($"Could not parse {typeof(T).Name} from value {tokenText}")));
            };
        }

        private static Parser<string> OperatorLexem(params string[] expectedLexems)
        {
            return textInput =>
            {
                return textInput
                        .NextOperatorLexem()
                        .FlatMap(lexem =>
                            expectedLexems.Contains(lexem)
                                ? Right<ParsingErrors, string>(lexem)
                                : Left<ParsingErrors, string>(
                                    textInput.MakeErrors(
                                        $"None of expected characters ['{string.Join("', '", expectedLexems)}'] is present")));
            };
        }

        private static Parser<TResult> EatTrailingSpaces<TResult>(Parser<TResult> parser)
        {
            return from result in parser
                   from trailingSpaces in new Parser<int>(TextInput.EatWhiteSpaces)
                   select result;
        }

        private static bool CopyString(string s, out string result)
        {
            result = s;
            return true;
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
            Trace.WriteLine("=========" + ToString());
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

        public ParsingResult<string> NextToken()
        {
            var firstNonSpaceCharPos = SkipLeadingSpaces();
            if (firstNonSpaceCharPos == null)
            {
                return new ParsingResult<string>(MakeErrors("No more tokens in the input."));
            }

            var operationLexem = TryScanOperationLexem(firstNonSpaceCharPos.Value);
            if (operationLexem != null)
            {
                return operationLexem.Value;
            }

            var firstNonTokenCharPos = ScanMultiCharToken(firstNonSpaceCharPos.Value);
            return new ParsingResult<string>(
                _text.Substring(firstNonSpaceCharPos.Value, firstNonTokenCharPos - firstNonSpaceCharPos.Value),
                SkipTo(firstNonTokenCharPos));
        }

        public ParsingResult<string> NextQuotedToken(char openingQuoteChar, char closingQuoteChar)
        {
            var firstNonSpaceCharPos = SkipLeadingSpaces();
            if (firstNonSpaceCharPos == null)
            {
                return new ParsingResult<string>(MakeErrors("No more tokens in the input."));
            }

            var firstTokenChar = _text[firstNonSpaceCharPos.Value];
            if (firstTokenChar != openingQuoteChar)
            {
                return new ParsingResult<string>(MakeErrors($"Expected {openingQuoteChar}."));
            }

            var tokenLengthOrError = ScanQuotedToken(firstNonSpaceCharPos.Value, closingQuoteChar);
            var localThis = this;
            return tokenLengthOrError.Map(
                tokenLength => localThis._text
                    .Substring(firstNonSpaceCharPos.Value + 1, tokenLength)
                    .Replace("\\" + openingQuoteChar, openingQuoteChar.ToString())
                    .Replace("\\" + closingQuoteChar, closingQuoteChar.ToString()));
        }

        public ParsingResult<string> NextOperatorLexem()
        {
            var firstNonSpaceCharPos = SkipLeadingSpaces();
            if (firstNonSpaceCharPos == null)
            {
                return new ParsingResult<string>(MakeErrors("No more tokens in the input."));
            }

            return TryScanOperationLexem(firstNonSpaceCharPos.Value) 
                 ?? new ParsingResult<string>(MakeErrors($"Next token is not an operation lexem {SkipTo(firstNonSpaceCharPos.Value)}."));
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

        public static ParsingResult<string> Identifier(TextInput input)
        {
            var firstNonSpaceCharPos = input.SkipLeadingSpaces();
            if (firstNonSpaceCharPos == null)
            {
                return new ParsingResult<string>(input.MakeErrors("Identifier expected."));
            }

            if (!char.IsLetter(input._text[firstNonSpaceCharPos.Value]))
            {
                return new ParsingResult<string>(input.MakeErrors("The first character of an identifier should be a letter."));
            }

            var i = firstNonSpaceCharPos.Value + 1;
            for (; i < input._text.Length && char.IsLetterOrDigit(input._text[i]); ++i)
            {
            }

            var identifierLength = i - firstNonSpaceCharPos.Value;
            return new ParsingResult<string>(input._text.Substring(firstNonSpaceCharPos.Value, identifierLength), input.SkipTo(i));
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

        private ParsingResult<string>? TryScanOperationLexem(int start)
        {
            var localText = _text;
            var matchingLexemIndex = Array.FindIndex(
                OperatorLexems,
                l => string.Compare(l, 0, localText, start, l.Length, StringComparison.Ordinal) == 0);
            if (matchingLexemIndex < 0)
            {
                return null;
            }

            var lexem = OperatorLexems[matchingLexemIndex];
            return new ParsingResult<string>(lexem, SkipTo(start + lexem.Length));
        }

        private int ScanMultiCharToken(int tokenStartPos)
        {
            for (var i = tokenStartPos + 1; i < _text.Length; ++i)
            {
                var ch = _text[i];
                if (char.IsWhiteSpace(ch) || Array.Exists(OperatorLexems, lexem => lexem.StartsWith(ch)))
                {
                    return i;
                }
            }

            return _text.Length;
        }

        private ParsingResult<int> ScanQuotedToken(int openingQuotePos, char closingQuoteChar)
        {
            for (var startIndex = openingQuotePos + 1; startIndex < _text.Length; )
            {
                var nextQuotePos = _text.IndexOf(closingQuoteChar, startIndex);
                if (nextQuotePos < 0)
                {
                    return new ParsingResult<int>(
                        MakeErrors(
                            $"Quoted token {_text.Substring(openingQuotePos)} does not have closing {closingQuoteChar} character."));
                }

                if (_text[nextQuotePos - 1] != '\\')
                {
                    return new ParsingResult<int>(nextQuotePos - openingQuotePos - 1, new TextInput(_text, nextQuotePos + 1));
                }

                startIndex = nextQuotePos + 1;
            }

            return new ParsingResult<int>(
                MakeErrors(
                    $"Quoted token {_text.Substring(openingQuotePos)} does not have closing {closingQuoteChar} character."));
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

        public ParsingResult<TMappedResult> FlatMap<TMappedResult>(
            Func<TResult, Either<ParsingErrors, TMappedResult>> mapValue)
        {
            return _result.Fold(
                        errors => new ParsingResult<TMappedResult>(errors),
                        parsingResult => new ParsingResult<TMappedResult>(
                            mapValue(parsingResult.ParsedValue).Map(mappedValue =>
                                            (mappedValue, parsingResult.RemainingInput))));
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

        public static Parser<TResult> StopParsingIfFailed<TResult>(this Parser<TResult> @this)
        {
            return textInput => @this(textInput).OrElse(error => new ParsingResult<TResult>(error.MakeFatal()));
        }

        public static Parser<TResult> AsParser<TResult>(
            TryParse<TResult> tryParse,
            string text,
            Func<string, string> makeErrorMessage)
        {
            return textInput =>
                tryParse(text, out var result)
                        ? new ParsingResult<TResult>(result, textInput)
                        : new ParsingResult<TResult>(textInput.MakeErrors(makeErrorMessage(text)));
        }

        private static Parser<IReadOnlyCollection<TResult>> Repeat<TResult>(Parser<TResult> parser)
        {
            return (from headElement in parser
                    from tailElements in Repeat(parser)
                    select Concatenate(headElement, tailElements))
                   .Or(Success((IReadOnlyCollection<TResult>)new List<TResult>()));
        }
    }
}