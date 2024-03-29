﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static FormulaParser.Tests.Either;
using static FormulaParser.Tests.Maybe;
using static FormulaParser.Tests.CellValueCalculator;
using static FormulaParser.Tests.ParserExtensions;
using static FormulaParser.Tests.TextInput;
using AggregatedValues = System.Collections.Generic.IDictionary<string, object>;
using AggregatedValueCalculators = System.Collections.Immutable.ImmutableList<System.Collections.Generic.KeyValuePair<string, System.Linq.Expressions.Expression>>;

namespace FormulaParser.Tests
{
    public sealed class ColumnFormula
    {
        internal ColumnFormula(
            CellValueCalculator cellValueCalculator, 
            FormulaExpressionsCompiler formulaExpressionsCompiler)
        {
            _cellCalculator = formulaExpressionsCompiler.CompileCellValueCalculator(cellValueCalculator);
            _aggregatedValueCalculators = formulaExpressionsCompiler.CompileAggregatedValueCalculators(cellValueCalculator.AggregatedValuesCalculators);
            _expressionText = cellValueCalculator.ToString();
        }

        public AggregatedValues CalculateAggregatedValues(IEnumerable rows)
        {
            var result = new Dictionary<string, object>();
            foreach (var aggregatedValueCalculator in _aggregatedValueCalculators)
            {
                if (!result.ContainsKey(aggregatedValueCalculator.Key))
                {
                    result.Add(aggregatedValueCalculator.Key, aggregatedValueCalculator.Value.DynamicInvoke(result, rows));
                }
            }

            return result;
        }

        public object CalculateCellValue(object row, AggregatedValues aggregatedValues) =>
            _cellCalculator.DynamicInvoke(row, aggregatedValues);

        public override string ToString() => _expressionText;

        private readonly Delegate _cellCalculator;
        private readonly IReadOnlyCollection<KeyValuePair<string, Delegate>> _aggregatedValueCalculators;
        private readonly string _expressionText;
    }

    public class ColumnFormulaBuilder
    {
        public ColumnFormulaBuilder(
            Type rowDataType, 
            TryParse<Type> tryGetPropertyTypeByName, 
            TryParse<IReadOnlyCollection<MethodInfo>> tryGetFunctionsByName)
        {
            var parameters = new FormulaParameters(rowDataType);
            _formulaCompiler = new FormulaExpressionsCompiler(parameters);

            var integerLiteral = NumericLiteral(new TryParse<int>(int.TryParse));
            var decimalLiteral = NumericLiteral(new TryParse<decimal>(decimal.TryParse));
            var dateTimeLiteral = ParsedQuotedLiteral(new TryParse<DateTime>(DateTime.TryParse));
            var stringLiteral = QuotedLiteral.Select(Constant);
            var literal = dateTimeLiteral.Or(stringLiteral).Or(integerLiteral).Or(decimalLiteral);

            var quotedPropertyName = from openingBracket in Lexem("[")
                                     from nameChars in Repeat(ch => ch != ']' && ch != '\r' && ch != '\n')
                                     from closingBraket in Lexem("]")
                                     select string.Join(
                                         string.Empty, 
                                         nameChars
                                            .Where(ch => ch != ' ' && ch != '\t')
                                            .Select(ch => char.IsLetterOrDigit(ch) ? ch.ToString() : $"_char_{(int)ch}_"));
            var propertyName = quotedPropertyName.Or(Identifier);
            var propertyAccessor = from name in propertyName
                                   from propertyType in name.Try(tryGetPropertyTypeByName, n => $"Unknown property: '{n}'").StopParsingIfFailed()
                                   select new CellValueCalculator(propertyType, Expression.Property(parameters.CurrentRow, name));

            var aggregatableExpression = CreateFormulaParser(literal, propertyAccessor, tryGetFunctionsByName, parameters);

            var aggregatedPropertyAccessor = from openingBracket in Lexem("[")
                                             from aggregationMethodText in Identifier
                                             from colon in Lexem(":")
                                             from aggregationMethod in aggregationMethodText.Try<AggregationMethod>(
                                                 Enum.TryParse, 
                                                 m => $"Invalid aggregation method {m} specified: only '" + 
                                                      string.Join("', '", Enum.GetNames(typeof(AggregationMethod)) + 
                                                      "' are supported")).StopParsingIfFailed()
                                             from calculator in aggregatableExpression
                                             from closingBraket in Lexem("]")
                                             select aggregationMethod == AggregationMethod.all
                                                ? calculator.All(_formulaCompiler)
                                                : calculator.FirstOrLast(aggregationMethod, _formulaCompiler);

            _formulaTextParser = CreateFormulaParser(
                literal,
                aggregatedPropertyAccessor.Or(propertyAccessor),
                tryGetFunctionsByName,
                parameters);
        }

        public ColumnFormula Build(string formulaText)
        {
            var parsingResult = EatTrailingSpaces(_formulaTextParser)(new TextInput(formulaText));
            return parsingResult
                    .GetCompleteOrElse(
                        valueCalculator => new ColumnFormula(valueCalculator, _formulaCompiler), 
                        parsingErrors => throw parsingErrors.AsException(formulaText));
        }

        private static Parser<CellValueCalculator> CreateFormulaParser(
            Parser<CellValueCalculator> literal,
            Parser<CellValueCalculator> propertyAccessor,
            TryParse<IReadOnlyCollection<MethodInfo>> tryGetFunctionByName,
            FormulaParameters formulaParameters)
        {
            Parser<CellValueCalculator> resultingParser = null;

            // ReSharper disable once AccessToModifiedClosure
            var parameterList = from openingBrace in Lexem("(")
                                from arguments in Optional(UniformList(resultingParser, Lexem(",")))
                                from closingBrace in Lexem(")")
                                select arguments.Map(a => a.Elements).OrElse(Enumerable.Empty<CellValueCalculator>());

            TryParse<Maybe<IReadOnlyCollection<MethodInfo>>> tryGetFunctionOrConditionExpressionByName = 
                (string name, out Maybe<IReadOnlyCollection<MethodInfo>> result) =>
                {
                    result = tryGetFunctionByName(name, out var methodInfos) ? Some(methodInfos) : None;
                    return result.Map(_ => true).OrElse(name == "Iif" || name == "If");
                };

            var functionCall = from functionName in Identifier
                               from lazyParameters in parameterList
                               from methodInfos in functionName.Try(tryGetFunctionOrConditionExpressionByName, n => $"Unknown function: '{n}'").StopParsingIfFailed()
                               let parameters = lazyParameters.ToArray()
                               from methodInfo in PickFunctionOverload(functionName, methodInfos, parameters).StopParsingIfFailed()
                               from arguments in methodInfo
                                                    .Map(mi => MakeFunctionCallArguments(mi, parameters))
                                                    .OrElse(() => MakeIifFunctionCallArguments(parameters))
                                                    .StopParsingIfFailed()
                               select CombineCalculators(
                                   methodInfo.Map(mi => mi.ReturnType).OrElse(() => arguments[1].Type),
                                   formulaParameters,
                                   methodInfo.Map(mi => (Expression)Expression.Call(mi, arguments))
                                             .OrElse(() => Expression.Condition(arguments[0], arguments[1], arguments[2])),
                                   $"{functionName}({string.Join(",", parameters.Select(c => c.CalculateExpression))})",
                                   parameters);

            // ReSharper disable once AccessToModifiedClosure
            var bracedExpression = from openingBrace in Lexem("(")
                                   from internalExpression in resultingParser
                                   from closingBrace in Lexem(")")
                                   select internalExpression;

            var multiplier = from optionalSign in Optional(Lexem("-", "+"))
                             from valueCalculator in literal.Or(bracedExpression).Or(functionCall).Or(propertyAccessor)
                             from adjustedCalculator in AsParser(valueCalculator.TryGiveSign(optionalSign.OrElse(string.Empty)))
                             select adjustedCalculator;

            resultingParser = CreateFormulaParserCore(multiplier, formulaParameters);

            return resultingParser;
        }

        private static Parser<CellValueCalculator> CreateFormulaParserCore(
            Parser<CellValueCalculator> multiplier, 
            FormulaParameters parameters)
        {
            var addend = ArithmeticExpressionParser(multiplier, parameters, "*", "/", "%");
            var comparableExpression = ArithmeticExpressionParser(addend, parameters, "+", "-");
            var equatableExpression = ArithmeticExpressionParser(comparableExpression, parameters, "<", "<=", ">", ">=");
            var bitwiseAndableExpression = ArithmeticExpressionParser(equatableExpression, parameters, "==", "!=");
            var bitwiseXorableExpression = ArithmeticExpressionParser(bitwiseAndableExpression, parameters, "&");
            var bitwiseOrableExpression = ArithmeticExpressionParser(bitwiseXorableExpression, parameters, "^");
            var logicalAndableExpression = ArithmeticExpressionParser(bitwiseOrableExpression, parameters, "|");
            var logicalOrableExpression = ArithmeticExpressionParser(logicalAndableExpression, parameters, "&&");
            return ArithmeticExpressionParser(logicalOrableExpression, parameters, "||");
        }

        private static Parser<Maybe<MethodInfo>> PickFunctionOverload(
            string functionName,
            Maybe<IReadOnlyCollection<MethodInfo>> methodInfos,
            CellValueCalculator[] valueCalculators)
        {
            return methodInfos.Map(mis =>
                {
                    var actualParameterTypes = valueCalculators.Select(c => c.ResultType).ToArray();
                    var matchingMethod = mis
                        .FirstOrDefault(mi => mi.GetParameters().Select(p => p.ParameterType).SequenceEqual(actualParameterTypes))
                        ?? mis.FirstOrDefault(mi => mi.GetParameters().Length == actualParameterTypes.Length);
                    return matchingMethod == null
                        ? Failure<Maybe<MethodInfo>>(textInput => textInput.MakeErrors(
                            $"Could not find overload of function '{functionName}' matching argument types " + 
                            $"({string.Join(",", actualParameterTypes.Select(t => t.Name))})"))
                        : Success(Some(matchingMethod));
                })
                .OrElse(Success<Maybe<MethodInfo>>(None));
        }

        private static Parser<Expression[]> MakeFunctionCallArguments(
            MethodInfo methodInfo, 
            IReadOnlyCollection<CellValueCalculator> parameterCalculators)
        {
            var parameters = methodInfo.GetParameters();
            if (parameters.Length != parameterCalculators.Count)
            {
                return Failure<Expression[]>(textInput =>
                    textInput.MakeErrors($"Function {methodInfo.Name} expects {parameters.Length} parameters, {parameterCalculators.Count} provided."));
            }

            var argumentsConvertedToExpectedTypes = parameters
                .Zip(parameterCalculators, (parameter, parameterCaclulator) =>
                    new { parameter, valueCaclulator = parameterCaclulator.TryCastTo(parameter.ParameterType) })
                .ToArray();

            var conversionErrors = argumentsConvertedToExpectedTypes
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
                argumentsConvertedToExpectedTypes
                    .Select(item => item.valueCaclulator.Fold(
                        _ => throw new InvalidOperationException("Program logic error: by this time we should have made sure that all parameters are compatible by types."),
                        calculator => calculator.CalculateExpression))
                    .ToArray());
        }

        private static Parser<Expression[]> MakeIifFunctionCallArguments(CellValueCalculator[] parameterCalculators)
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
                from resultingType in TryToDeduceResultingType("Iif", parameterCalculators[1].ResultType, parameterCalculators[2].ResultType)
                from firstValue in parameterCalculators[1].TryCastTo(resultingType)
                from secondValue in parameterCalculators[2].TryCastTo(resultingType)
                select new[] { parameterCalculators[0].CalculateExpression, firstValue.CalculateExpression, secondValue.CalculateExpression };

            return realArgumentCalculators
                        .Fold(
                            error => Failure<Expression[]>(textInput => textInput.MakeErrors(error, null)),
                            Success);
        }

        private static Parser<CellValueCalculator> ArithmeticExpressionParser(
            Parser<CellValueCalculator> elementParser, 
            FormulaParameters parameters,
            params string[] operationLexems) 
            => 
            from elements in UniformList(elementParser, Operator(operationLexems))
            from combinedCalculator in FoldBinaryOperatorsList(elements, parameters).StopParsingIfFailed()
            select combinedCalculator;

        private static Parser<CellValueCalculator> FoldBinaryOperatorsList(
            ParsedUniformList<CellValueCalculator, string> operands, 
            FormulaParameters parameters) => 
            operands
                .TailPairs
                .Aggregate(
                    Right<ParsingError, CellValueCalculator>(operands.FirstElement), 
                    (valueCalculatorOrError, pair) =>
                        valueCalculatorOrError.FlatMap(calculator => calculator.TryApplyBinaryOperator(pair.Link, pair.Right, parameters)))
                .Fold(
                    error => Failure<CellValueCalculator>(textInput => textInput.MakeErrors(error, null)), 
                    Success);

        private static Parser<CellValueCalculator> NumericLiteral<TNumeric>(TryParse<TNumeric> tryParse) where TNumeric : struct => 
            from token in RegularToken
            from numeric in token.Try(tryParse, t => $"Could not parse {typeof(TNumeric).Name} from value {t}")
            select Constant(numeric);

        private static Parser<CellValueCalculator> ParsedQuotedLiteral<T>(TryParse<T> tryParse) =>
            from literal in QuotedLiteral
            from parsedLiteral in literal.Try(tryParse, t => $"Could not parse {typeof(T).Name} from value '{t}'")
            select Constant(parsedLiteral);

        private static Parser<TResult> EatTrailingSpaces<TResult>(Parser<TResult> parser) => 
            from result in parser
            from trailingSpaces in Repeat(char.IsWhiteSpace)
            select result;

        private readonly FormulaExpressionsCompiler _formulaCompiler;
        private readonly Parser<CellValueCalculator> _formulaTextParser;
    }

    internal readonly struct FormulaParameters
    {
        public FormulaParameters(Type rowDataType)
        {
            RowDataType = rowDataType;
            AllRows = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(rowDataType), "allRows");
            CurrentRow = Expression.Parameter(rowDataType, "currentRow");
            AggregatedValues = Expression.Parameter(typeof(AggregatedValues), "aggregatedValues");
        }

        public Type RowDataType { get; }

        public ParameterExpression AllRows { get; }

        public ParameterExpression CurrentRow { get; }

        public ParameterExpression AggregatedValues { get; }
    }

    internal sealed class FormulaExpressionsCompiler
    {
        public FormulaExpressionsCompiler(FormulaParameters formulaParameters)
        {
            Parameters = formulaParameters;
        }

        public FormulaParameters Parameters { get; }

        public Delegate CompileCellValueCalculator(CellValueCalculator valueCalculator)
        {
            return Expression.Lambda(valueCalculator.CalculateExpression, Parameters.CurrentRow, Parameters.AggregatedValues).Compile();
        }

        public IReadOnlyCollection<KeyValuePair<string, Delegate>> CompileAggregatedValueCalculators(AggregatedValueCalculators aggregatedValuesCalculators)
        {
            var calculatorsReturningEnumerablesThatAreUsedSeveralTimes = aggregatedValuesCalculators
                .Where(item => item.Value.Type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(item.Value.Type))
                .GroupBy(item => item.Key)
                .Where(item => item.HasMoreThanOneElement())
                .Select(item => item.Key)
                .ToArray();

            var result = new List<KeyValuePair<string, Delegate>>();
            foreach (var calculatorInfo in aggregatedValuesCalculators.Reverse())
            {
                if (result.Any(kvp => kvp.Key == calculatorInfo.Key))
                {
                    continue;
                }

                var expression = /*calculatorsReturningEnumerablesThatAreUsedSeveralTimes.Contains(calculatorInfo.Key)
                        ? ConvertToReadOnlyCollection(calculatorInfo.Value)
                        : */ calculatorInfo.Value;

                result.Add(KeyValuePair.Create(
                    calculatorInfo.Key,
                    Expression.Lambda(expression, Parameters.AggregatedValues, Parameters.AllRows).Compile()));
            }

            return result;
        }

        public Expression CallEnumerableSelect(Expression rowProjection, Type projectionType)
        {
            // Enumerable.Select<TRowDataType>(currentRow => rowProjection(currentRow))
            var selectMethodInfo = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(mi => mi.Name == "Select" && mi.GetParameters()[1].ParameterType.GetGenericArguments().Length == 2)
                .MakeGenericMethod(Parameters.RowDataType, projectionType);
            return Expression.Call(selectMethodInfo, Parameters.AllRows, Expression.Constant(CompileRowProjection(rowProjection)));
        }

        public Expression CallEnumerableFirstOrLast(Expression allProjectedRows, AggregationMethod aggregationMethod, Type elementType)
        {
            // Enumerable.FirstOrDefault<TRowDataType>(allProjectedRows)
            var methodName = aggregationMethod == AggregationMethod.first ? "FirstOrDefault" : "LastOrDefault";
            var firstOrLastMethodInfo = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(mi => mi.Name == methodName && mi.GetParameters().Length == 1)
                .MakeGenericMethod(elementType);
            return Expression.Call(firstOrLastMethodInfo, allProjectedRows);
        }

        private Delegate CompileRowProjection(Expression selectorExpression)
        {
            return Expression.Lambda(selectorExpression, Parameters.CurrentRow).Compile();
        }

        private static Expression ConvertToReadOnlyCollection(Expression expression)
        {
            // expression as IReadOnlyCollection<T> ?? expression.ToArray()
            bool IsGenericIEnumerable(Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>);

            var enumerableType = IsGenericIEnumerable(expression.Type)
                ? expression.Type
                : expression.Type.GetInterfaces().Single(IsGenericIEnumerable);

            var enumeratedType = enumerableType.GetGenericArguments().Single();

            var toArrayMethodInfo = typeof(Enumerable)
                .GetMethod("ToArray", BindingFlags.Public | BindingFlags.Static)
                .MakeGenericMethod(enumeratedType);

            return
                Expression.Coalesce(
                    Expression.TypeAs(expression, typeof(IReadOnlyCollection<>).MakeGenericType(enumeratedType)),
                    Expression.Call(toArrayMethodInfo, expression));
        }
    }

    internal readonly struct CellValueCalculator
    {
        public CellValueCalculator(
            Type resultType, 
            Expression calculateExpression,
            AggregatedValueCalculators aggregatedValueCalculators = null,
            bool canBeAggregated = false)
        {
            ResultType = resultType;
            CalculateExpression = calculateExpression;
            AggregatedValuesCalculators = aggregatedValueCalculators ?? AggregatedValueCalculators.Empty;
            _canBeAggregated = canBeAggregated;
        }

        public readonly Type ResultType;

        public readonly Expression CalculateExpression;

        public readonly AggregatedValueCalculators AggregatedValuesCalculators;

        public Either<ParsingError, CellValueCalculator> TryCastTo(Type type)
        {
            try
            {
                return Right(
                    ResultType == type
                        ? this
                        : new CellValueCalculator(
                            type, 
                            Expression.Convert(CalculateExpression, type),
                            AggregatedValuesCalculators,
                            _canBeAggregated));
            }
            catch (InvalidOperationException exception)
            {
                return Left(new ParsingError(exception.Message));
            }
        }

        public Either<ParsingError, CellValueCalculator> TryGiveSign(string sign)
        {
            if (!IsNumeric && !string.IsNullOrEmpty(sign))
            {
                return Left(new ParsingError($"Operator '{sign}' cannot be applied to operand of type '{ResultType.Name}'"));
            }

            return Right(
                sign != "-"
                    ? this
                    : new CellValueCalculator(
                        ResultType,
                        Expression.Multiply(
                            Expression.Constant(Convert.ChangeType(-1, ResultType), ResultType),
                            CalculateExpression),
                        AggregatedValuesCalculators,
                        _canBeAggregated));
        }

        public Either<ParsingError, CellValueCalculator> TryApplyBinaryOperator(
            string operatorChars, 
            CellValueCalculator secondArgument,
            FormulaParameters parameters)
        {
            if ((operatorChars == "&&" || operatorChars == "||") && 
                (ResultType != typeof(bool) || secondArgument.ResultType != typeof(bool)))
            {
                return Left(new ParsingError($"Operator '{operatorChars}' cannot be applied to operands of type '{ResultType.Name}' and '{secondArgument.ResultType}'"));
            }

            var op = KnownBinaryOperations[operatorChars];
            var localThis = this;
            return
                from resultType in TryToDeduceResultingType(operatorChars, ResultType, secondArgument.ResultType)
                from leftOperand in localThis.TryCastTo(resultType)
                from rightOperand in secondArgument.TryCastTo(resultType)
                select CombineCalculators(
                        op.ResultIsBoolean ? typeof(bool) : resultType,
                        parameters,
                        op.Expression(leftOperand.CalculateExpression, rightOperand.CalculateExpression),
                        $"{leftOperand.CalculateExpression}{operatorChars}{rightOperand.CalculateExpression}",
                        leftOperand,
                        rightOperand);
        }

        public CellValueCalculator All(FormulaExpressionsCompiler compiler)
        {
            var precalculatedAggregatedValueKey = "all:" + CalculateExpression;
            var exactEnumerableType = typeof(IEnumerable<>).MakeGenericType(ResultType);
            return new CellValueCalculator(
                exactEnumerableType,
                GetPrecalculatedAggregatedValue(exactEnumerableType, compiler.Parameters, precalculatedAggregatedValueKey),
                ImmutableList.Create(
                    KeyValuePair.Create(precalculatedAggregatedValueKey, compiler.CallEnumerableSelect(CalculateExpression, ResultType))),
                canBeAggregated: true);
        }

        public CellValueCalculator FirstOrLast(AggregationMethod aggregationMethod, FormulaExpressionsCompiler compiler)
        {
            var allRowsCalculator = All(compiler);
            var key = $"{aggregationMethod}:{CalculateExpression}";
            return new CellValueCalculator(
                ResultType,
                GetPrecalculatedAggregatedValue(ResultType, compiler.Parameters, key),
                allRowsCalculator.AggregatedValuesCalculators.Insert(
                    0,
                    KeyValuePair.Create(
                        key,
                        compiler.CallEnumerableFirstOrLast(
                            allRowsCalculator.CalculateExpression,
                            aggregationMethod,
                            ResultType))),
                canBeAggregated: true);
        }

        public override string ToString() =>
            $"{CalculateExpression}: '{ResultType.Name}'";

        public static CellValueCalculator Constant<T>(T value) => 
            new CellValueCalculator(typeof(T), Expression.Constant(value, typeof(T)), canBeAggregated: true);

        public static Either<ParsingError, Type> TryToDeduceResultingType(string operatorChars, Type type1, Type type2)
        {
            if (type1 == type2)
            {
                return Right(type1);
            }

            var i1 = Array.IndexOf(TypesConversionSequence, type1);
            var i2 = Array.IndexOf(TypesConversionSequence, type2);
            if (i1 < 0 || i2 < 0)
            {
                return Left(new ParsingError($"Operator '{operatorChars}' cannot be applied to operands of type '{type1.Name}' and '{type2.Name}'"));
            }

            return Right(i1 > i2 ? type1 : type2);
        }

        public static CellValueCalculator CombineCalculators(
            Type resultType,
            FormulaParameters parameters,
            Expression calculateExpression,
            string aggregatedValueKey,
            params CellValueCalculator[] calculators)
        {
            var allAggregatedValueCalculators = calculators.Aggregate(
                        AggregatedValueCalculators.Empty,
                        (combinedCalculators, calculator) => combinedCalculators.AddRange(calculator.AggregatedValuesCalculators));

            return !calculators.All(c => c._canBeAggregated)
                    ? new CellValueCalculator(
                        resultType,
                        calculateExpression,
                        allAggregatedValueCalculators)
                    : new CellValueCalculator(
                        resultType,
                        GetPrecalculatedAggregatedValue(resultType, parameters, aggregatedValueKey),
                        allAggregatedValueCalculators.Insert(0, KeyValuePair.Create(aggregatedValueKey, calculateExpression)),
                        canBeAggregated: true);
        }

        private static Expression GetPrecalculatedAggregatedValue(
            Type resultType,
            FormulaParameters parameters,
            string aggregatedValueKey)
        {
            // return (resultType)parameters.AggregatedValues[aggregatedValueKey]
            return Expression.Convert(
                Expression.Property(
                    parameters.AggregatedValues,
                    "Item",
                    Expression.Constant(aggregatedValueKey, typeof(string))),
                resultType);
        }

        private bool IsNumeric => TypesConversionSequence.Contains(ResultType);

        private readonly bool _canBeAggregated;

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

    internal readonly struct ParsingError
    {
        public ParsingError(string errorMessage)
        {
            _errorMessage = errorMessage;
        }

        public ParsingError Amend(Func<string, string> amendMessage) =>
            new ParsingError(amendMessage(_errorMessage));

        public override string ToString() => _errorMessage;

        private readonly string _errorMessage;
    }

    internal sealed class ParsingErrors 
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
            // System.Diagnostics.Trace.WriteLine("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  " + headError + " " + errorLocation);
        }

        public bool IsFatal { get; }

        public Exception AsException(string formulaText) =>
            new InvalidOperationException($"[{formulaText}] parsing errors: {Environment.NewLine}{ToString()}");

        public ParsingErrors MakeFatal() =>
            new ParsingErrors(_headError, _tailErrors, _errorLocation, true);

        public override string ToString() =>
            string.Join(Environment.NewLine, ListErrors(_tailErrors, _headError)) + " at " + _errorLocation.ToString();

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

    internal readonly struct TextInput
    {
        public TextInput(string text, int currentPosition = 0)
        {
            _text = text ?? string.Empty;
            _currentPosition = currentPosition;
        }

        public bool IsEmpty => _currentPosition == _text.Length;

        public ParsingErrors AsUnparsedInputError() =>
            MakeErrors($"There stil remains unparsed text '{_text.Substring(_currentPosition)}' at the end.");

        public ParsingErrors MakeErrors(string errorMessage) =>
            new ParsingErrors(errorMessage, this);

        public ParsingErrors MakeErrors(ParsingError headError, ParsingErrors tailErrors) =>
            new ParsingErrors(headError, tailErrors, this);

        public override string ToString() =>
            _text.Insert(_currentPosition, "^");

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

        public static Parser<string> Lexem(params string[] lexemsText) =>
            from leadingWhitespaces in Repeat(char.IsWhiteSpace)
            from text in Match(lexemsText)
            select text;

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

        public static Parser<string> Repeat(Func<char, bool> isExpectedChar, bool atLeastOnce = false) =>
            Repeat((_, ch) => isExpectedChar(ch), atLeastOnce);

        private static Parser<string> Repeat(Func<char?, char, bool> isExpectedChar, bool atLeastOnce) =>
            textInput =>
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

        private static Parser<string> Match(params string[] texts) =>
            textInput =>
            {
                var matchingTextIndex = Array.FindIndex(
                    texts,
                    text => string.Compare(textInput._text, textInput._currentPosition, text, 0, text.Length, StringComparison.Ordinal) == 0);
                return matchingTextIndex >= 0
                    ? new ParsingResult<string>(texts[matchingTextIndex], textInput.SkipTo(textInput._currentPosition + texts[matchingTextIndex].Length))
                    : new ParsingResult<string>(textInput.MakeErrors($"{(texts.Length > 1 ? "One of " : string.Empty)}'{(texts.Length > 1 ? string.Join(" ", texts) : texts.Single())}' expected"));
            };

        private TextInput SkipTo(int newPosition)
        {
            Assert.IsTrue(_currentPosition <= newPosition, "SkipTo goes back.");
            Assert.IsTrue(newPosition <= _text.Length, "SkipTo goes beyond the end of input.");
            return new TextInput(_text, newPosition);
        }

        private readonly string _text;
        private readonly int _currentPosition;

        private static readonly string[] OperatorLexems = new[] { "<=", ">=", "==", "!=", "&&", "||", "<", ">", ",", "(", ")", "[", "]", "+", "-", "*", "/", "%", "|", "&" };
        private static readonly char[] OperatorStartingCharacters = OperatorLexems.Select(op => op.First()).Distinct().ToArray();
    }

    internal readonly struct ParsingResult<TResult>
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

        public ParsingResult<TMappedResult> Map<TMappedResult>(Func<TResult, TMappedResult> mapValue) =>
            new ParsingResult<TMappedResult>(
                _result.Map(parsingResult => (mapValue(parsingResult.ParsedValue), parsingResult.RemainingInput)));

        public ParsingResult<TValue2> SelectMany<TIntermediate, TValue2>(
            Func<TResult, Parser<TIntermediate>> selector,
            Func<TResult, TIntermediate, TValue2> projector) 
            =>
            new ParsingResult<TValue2>(
                from thisResult in _result
                from intermediateResult in selector(thisResult.ParsedValue)(thisResult.RemainingInput)._result
                select (projector(thisResult.ParsedValue, intermediateResult.ParsedValue), intermediateResult.RemainingInput));

        public TFinalResult GetCompleteOrElse<TFinalResult>(
            Func<TResult, TFinalResult> mapFinalResult,
            Func<ParsingErrors, TFinalResult> processErrors) 
            =>
            _result.Fold(
                processErrors,
                parsingResult => parsingResult.RemainingInput.IsEmpty
                                    ? mapFinalResult(parsingResult.ParsedValue)
                                    : processErrors(parsingResult.RemainingInput.AsUnparsedInputError()));

        public ParsingResult<TResult> OrElse(Func<ParsingErrors, ParsingResult<TResult>> getDefaultValue) =>
            _result.Fold(
                error => error.IsFatal ? new ParsingResult<TResult>(error) : getDefaultValue(error),
                result => new ParsingResult<TResult>(result.ParsedValue, result.RemainingInput));

        public override string ToString() => _result.ToString();

        private readonly Either<ParsingErrors, (TResult ParsedValue, TextInput RemainingInput)> _result;
    }

    internal readonly struct ParsedUniformList<TElement, TLink>
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

    internal delegate ParsingResult<TResult> Parser<TResult>(TextInput input);

    public delegate bool TryParse<T>(string s, out T result);

    // ReSharper disable InconsistentNaming
    internal enum AggregationMethod { all, first, last }
    // ReSharper enable InconsistentNaming

    internal static class ParserExtensions
    {
        public static Parser<TResult> Success<TResult>(TResult result) =>
            textInput => new ParsingResult<TResult>(result, textInput);

        public static Parser<TResult> Failure<TResult>(Func<TextInput, ParsingErrors> makeErrors) =>
            textInput => new ParsingResult<TResult>(makeErrors(textInput));

        public static Parser<TResult> StopParsingIfFailed<TResult>(this Parser<TResult> @this) =>
            textInput => @this(textInput).OrElse(error => new ParsingResult<TResult>(error.MakeFatal()));

        public static Parser<TValue2> Select<TValue, TValue2>(this Parser<TValue> @this, Func<TValue, TValue2> selector) =>
            textInput => @this(textInput).Map(selector);

        public static Parser<TValue2> SelectMany<TValue, TIntermediate, TValue2>(
            this Parser<TValue> @this,
            Func<TValue, Parser<TIntermediate>> selector,
            Func<TValue, TIntermediate, TValue2> projector) 
            =>
            textInput => @this(textInput).SelectMany(selector, projector);

        public static Parser<TResult> Try<TResult>(
            this string @this,
            TryParse<TResult> tryParse,
            Func<string, string> makeErrorMessage) 
            =>
            tryParse(@this, out var r) ? Success(r) : Failure<TResult>(textInput => textInput.MakeErrors(makeErrorMessage(@this)));

        public static Parser<Maybe<TResult>> Optional<TResult>(Parser<TResult> @this) =>
            textInput =>
            {
                return @this(textInput)
                        .Map(Some)
                        .OrElse(_ => new ParsingResult<Maybe<TResult>>(None, textInput));
            };

        public static Parser<TResult> Or<TResult>(this Parser<TResult> first, Parser<TResult> second) =>
            textInput =>
            {
                return first(textInput).OrElse(_ => second(textInput));
            };

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

        private static Parser<ImmutableList<TResult>> Repeat<TResult>(Parser<TResult> parser, bool atLeastOnce = false)
        {
            var result = from headElement in parser
                         from tailElements in Repeat(parser)
                         select tailElements.Insert(0, headElement);
            return atLeastOnce ? result : result.Or(Success(ImmutableList<TResult>.Empty));
        }
    }
}