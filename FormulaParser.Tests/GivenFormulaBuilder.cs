using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using JetBrains.Annotations;
using static FormulaParser.Tests.FormuaTextUtilities;

namespace FormulaParser.Tests
{
    [TestClass]
    public class GivenFormulaBuilder
    {
        [TestInitialize]
        public void Setup()
        {
            _testee = new RowFormulaBuilder(typeof(PropertyHolder), PropertyHolder.TryGetPropertyType, StockFunctions.TryGetFunction);
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseIntegerLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "0", "1", "-1", (int.MinValue + 1).ToString(), int.MaxValue.ToString() }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(int.Parse(formulaText), (int)formula.Apply(null, null), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseDecimalLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "1.0", "-1.0", decimal.MinValue.ToString(CultureInfo.InvariantCulture), decimal.MaxValue.ToString(CultureInfo.InvariantCulture) }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(decimal.Parse(formulaText), (decimal)formula.Apply(null, null), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseDateTimeLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "'2018-12-10 10:47:03Z'", "'2018-12-10 10:47:03'" }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(DateTime.Parse(formulaText.Trim(' ', '\t', '\'')), (DateTime)formula.Apply(null, null), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseStringLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "", " ", @"\'", @"   \'     ", "f", "Ö", "hello!", @"f-w\'ggh\'ffjfjfjf\'ggg" }))
            {
                var formula = _testee.Build($"'{formulaText}'");
                Assert.AreEqual(formulaText.Replace(@"\'", "'"), (string)formula.Apply(null, null), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParsePropertyAccessors()
        {
            var propertyHolder = new PropertyHolder { Book = Guid.NewGuid().ToString(), InstSubType = SubType.Irregular, I = 42, r = 3.1416M };
            foreach (var formulaText in SurroundWithSpaces(new[] { "Book", "[InstSubType]", "[Inst Sub Type]", "[ Inst Sub Type ]", "I", "[r]", "[FX Q->U]" }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(
                    propertyHolder.GetType().GetProperty(formulaText.Trim('[', ' ', ']', '\t').Replace(" ", string.Empty).Replace("->", "_char_45__char_62_")).GetValue(propertyHolder), 
                    formula.Apply(propertyHolder, null), 
                    $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsForInvalidPropertyAccessors()
        {
            foreach (var formula in new[] 
                {
                    "MissingProp",
                    "[MissingProp]", 
                    "[Missing Prop]", 
                    "MissingProp + 100",
                    "100 - [ Missing Prop]", 
                    "f() / [MissingProp]"
                })
            {
                try
                {
                    _testee.Build(formula);
                    Assert.Fail("Parsing" + Environment.NewLine + "\t" + formula + "should have failed.");
                }
                catch (InvalidOperationException exception)
                {
                    var expectedExceptionText = "Unknown property: 'MissingProp'";
                    Assert.IsTrue(
                        exception.Message.Contains(expectedExceptionText),
                        "Exception message " + Environment.NewLine + "    " +
                        exception.Message + Environment.NewLine +
                        "was supposed to contain the text" + Environment.NewLine + "    " +
                        expectedExceptionText + Environment.NewLine +
                        "but it does not.");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseFunctionCalls()
        {
            foreach (var functionCall in new[] 
                {
                 // exact parameter types match
                   new { FormulaText = "|f|(|)|", ExpectedResult = (object)StockFunctions.f() },
                   new { FormulaText = "|foo|(|1|)|", ExpectedResult = (object)StockFunctions.foo(1) },
                   new { FormulaText = "|bar|(|'qwerty'|,|[Book]|)|", ExpectedResult = (object)StockFunctions.bar("qwerty", DefaultPropertyHolder.Book) },
                   new { FormulaText = "|MakeFoo|(|bar|(|'asdf'|,|[Book]|)|,|foo|(|100|)|,|'2018-12-10 10:47:03Z'|)|", ExpectedResult = (object)StockFunctions.MakeFoo(StockFunctions.bar("asdf", DefaultPropertyHolder.Book), StockFunctions.foo(100), DateTime.Parse("2018-12-10 10:47:03Z")) },
                 // implicit conversions
                   new { FormulaText = "|foo|(|1.567|)|", ExpectedResult = (object)StockFunctions.foo((int)1.567M) },
                   new { FormulaText = "|MakeFoo|(|bar|(|'asdf'|,|[Book]|)|,|f|(|)|,|'2018-12-10 10:47:03Z'|)|", ExpectedResult = (object)StockFunctions.MakeFoo(StockFunctions.bar("asdf", DefaultPropertyHolder.Book), StockFunctions.f(), DateTime.Parse("2018-12-10 10:47:03Z")) },
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(functionCall.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(
                        functionCall.ExpectedResult,
                        formula.Apply(DefaultPropertyHolder, formula.CalculateAggregates(new[] { DefaultPropertyHolder })),
                        $"Formula text: [{formulaTextWithSpaces}]");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsForInvalidFunctionCalls()
        {
            foreach (var functionCall in new[]
                {
                  // Unknown function
                  new { FormulaText = "|MissingFunc|(|100|)|", ExpectedExceptionText = "Unknown function: 'MissingFunc'" },

                  // Incorrect number of parameters.
                   new { FormulaText = "|f|(|100|)|", ExpectedExceptionText = "Function f expects 0 parameters, 1 provided" },
                   new { FormulaText = "|bar|(|'qwerty'|)|", ExpectedExceptionText = "Function bar expects 2 parameters, 1 provided" },

                  // Incompatible argument types. 
                   new { FormulaText = "|bar|(|'qwerty'|,|100|)|", ExpectedExceptionText = "Argument of a wrong type was used for parameter 'right' of the function bar()" },
                   new { FormulaText = "|MakeFoo|(|'qwerty'|,|'2018-12-10 10:47:03Z'|,|5.567|)|", ExpectedExceptionText = "Argument of a wrong type was used for parameter 'p2' of the function MakeFoo()" },

                  // Incompatible resturn types
                  new { FormulaText = "|MakeFoo|(|'qwerty'|,|65.432|,|foo|(|1|)|)|", ExpectedExceptionText = "Argument of a wrong type was used for parameter 'p3' of the function MakeFoo()" },
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(functionCall.FormulaText))
                {
                    try
                    {
                        _testee.Build(formulaTextWithSpaces);
                        Assert.Fail("Parsing" + Environment.NewLine + "\t" + formulaTextWithSpaces + "should have failed.");
                    }
                    catch (InvalidOperationException exception)
                    {
                        Assert.IsTrue(
                            exception.Message.Contains(functionCall.ExpectedExceptionText), 
                            "Exception message " + Environment.NewLine + "    " +
                            exception.Message + Environment.NewLine + 
                            "was supposed to contain the text" + Environment.NewLine + "    " +
                            functionCall.ExpectedExceptionText + Environment.NewLine + 
                            "but it does not.");
                    }
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseMultiplyingStatements()
        {
            foreach (var invocation in new[] 
                {
                    new { FormulaText = "|2|*|3|", ExpectedResult = (object)(2 * 3) },
                    new { FormulaText = "|8|/|3|", ExpectedResult = (object)(8 / 3) },
                    new { FormulaText = "|6.6666|*|3.0|", ExpectedResult = (object)(6.6666M * 3.0M) },
                    new { FormulaText = "|6.6666|*|3|", ExpectedResult = (object)(6.6666M * 3) },
                    new { FormulaText = "|10|/|3.3333|", ExpectedResult = (object)(10/3.3333M) },
                    new { FormulaText = "|2|*|3|*|6|*|10|/|12|", ExpectedResult = (object)(2*3*6*10/12) },

                    new { FormulaText = "|f|(|)|*|3|", ExpectedResult = (object)(StockFunctions.f() * 3) },
                    new { FormulaText = "|5.456|/|foo|(|1|)|*|3|", ExpectedResult = (object)(5.456M/StockFunctions.foo(1) * 3) }
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(invocation.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(
                        invocation.ExpectedResult, 
                        formula.Apply(DefaultPropertyHolder, formula.CalculateAggregates(new[] { DefaultPropertyHolder })), 
                        $"Formula text: {formulaTextWithSpaces}");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseAddingStatements()
        {
            foreach (var invocation in new[]
                {
                    new { FormulaText = "|2|+|3|", ExpectedResult = (object)(2 + 3) },
                    new { FormulaText = "|8|-|3|", ExpectedResult = (object)(8 - 3) },
                    new { FormulaText = "|-|6.6666|+|3.0|", ExpectedResult = (object)(-6.6666M + 3.0M) },
                    new { FormulaText = "|6.6666|+|3|", ExpectedResult = (object)(6.6666M + 3) },
                    new { FormulaText = "|10|-|3.3333|", ExpectedResult = (object)(10 - 3.3333M) },
                    new { FormulaText = "|2|+|3|+|6|+|10|-|12|", ExpectedResult = (object)(2 + 3 + 6 + 10 - 12) },

                    new { FormulaText = "|2|+|3|*|6|+|10|/|2|", ExpectedResult = (object)(2 + 3 * 6 + 10 / 2) },
                    new { FormulaText = "|2|*|3|+|60|/|12|*|3|-|100|", ExpectedResult = (object)(2 * 3 + 60 / 12 * 3 - 100) },

                    new { FormulaText = "|f|(|)|+|3|", ExpectedResult = (object)(StockFunctions.f() + 3) },
                    new { FormulaText = "|5.456|-|foo|(|1|)|+|3|", ExpectedResult = (object)(5.456M - StockFunctions.foo(1) + 3) }
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(invocation.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(
                        invocation.ExpectedResult, 
                        formula.Apply(DefaultPropertyHolder, formula.CalculateAggregates(new[] { DefaultPropertyHolder })), 
                        $"Formula text: {formulaTextWithSpaces}");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsForInvalidComparizonChains()
        {
            foreach (var comparizonChain in new[]
                {
                   new { FormulaText = "|1|<=|2|>|3|", ExpectedExceptionText = "Operator '>' cannot be applied to operands of type 'Boolean' and 'Int32'" },
                   new { FormulaText = "|I|>=|f|(|)|<|3.3|", ExpectedExceptionText = "Operator '<' cannot be applied to operands of type 'Boolean' and 'Decimal'" },
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(comparizonChain.FormulaText))
                {
                    try
                    {
                        _testee.Build(formulaTextWithSpaces);
                        Assert.Fail("Parsing" + Environment.NewLine + "\t" + formulaTextWithSpaces + "should have failed.");
                    }
                    catch (InvalidOperationException exception)
                    {
                        Assert.IsTrue(
                            exception.Message.Contains(comparizonChain.ExpectedExceptionText),
                            "Exception message " + Environment.NewLine + "    " +
                            exception.Message + Environment.NewLine +
                            "was supposed to contain the text" + Environment.NewLine + "    " +
                            comparizonChain.ExpectedExceptionText + Environment.NewLine +
                            "but it does not.");
                    }
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseComparizonStatements()
        {
            foreach (var comparisons in new[]
                {
                    new { FormulaText = "|1|<|2|", ExpectedResult = (object)(1 < 2) },
                    new { FormulaText = "|8|-|3|<=|9|-|5|", ExpectedResult = (object)(8 - 3 <= 9 - 5) },
                    new { FormulaText = "|-|6.6666|>|3.0|", ExpectedResult = (object)(-6.6666M > 3.0M) },
                    new { FormulaText = "|6.6666|>=|3|", ExpectedResult = (object)(6.6666M >= 3) },
                    new { FormulaText = "|10|-|3.3333|<|25|*|-5|", ExpectedResult = (object)(10 - 3.3333M < 25 * -5) },
                    new { FormulaText = "|2|+|3|+|6|>=|10|-|12|", ExpectedResult = (object)(2 + 3 + 6 >= 10 - 12) },

                    new { FormulaText = "|2|+|3|*|6|<|10|/|2|", ExpectedResult = (object)(2 + 3 * 6 < 10 / 2) },
                    new { FormulaText = "|2|*|3|>|60|/|12|*|3|-|100|", ExpectedResult = (object)(2 * 3 > 60 / 12 * 3 - 100) },

                    new { FormulaText = "|f|(|)|>=|3|", ExpectedResult = (object)(StockFunctions.f() >= 3) },
                    new { FormulaText = "|5.456|<|foo|(|1|)|+|3|", ExpectedResult = (object)(5.456M < StockFunctions.foo(1) + 3) }
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(comparisons.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(
                        comparisons.ExpectedResult, 
                        formula.Apply(DefaultPropertyHolder, formula.CalculateAggregates(new[] { DefaultPropertyHolder })), 
                        $"Formula text: {formulaTextWithSpaces}");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsWhenUsingNonBooleanValuesInBooleanExpressions()
        {
            foreach (var booleanFormula in new[]
                {
                    new { FormulaText = "|1|~or~|3|>|5|", ExpectedExceptionText = "Operator '||' cannot be applied to operands of type 'Int32' and 'System.Boolean'" },
                    new { FormulaText = "|8.5|&&|3.0|==|3|", ExpectedExceptionText = "Operator '&&' cannot be applied to operands of type 'Decimal' and 'System.Boolean'" }
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(booleanFormula.FormulaText))
                {
                    try
                    {
                        _testee.Build(formulaTextWithSpaces);
                        Assert.Fail("Parsing" + Environment.NewLine + "\t" + formulaTextWithSpaces + "should have failed.");
                    }
                    catch (InvalidOperationException exception)
                    {
                        Assert.IsTrue(
                            exception.Message.Contains(booleanFormula.ExpectedExceptionText),
                            "Exception message " + Environment.NewLine + "    " +
                            exception.Message + Environment.NewLine +
                            "was supposed to contain the text" + Environment.NewLine + "    " +
                            booleanFormula.ExpectedExceptionText + Environment.NewLine +
                            "but it does not.");
                    }
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseBooleanExpressions()
        {
            foreach (var booleanFormula in new[]
                {
// ReSharper disable RedundantLogicalConditionalExpressionOperand
                    new { FormulaText = "|1|<|3|~or~|3|>|5|", ExpectedResult = (object)(1 < 3 || 3 > 5) },
                    new { FormulaText = "|8|-|3|<=|9|-|5|&&|3.0|==|3|", ExpectedResult = (object)(8 - 3 <= 9 - 5 && 3M == 3) }
// ReSharper enable RedundantLogicalConditionalExpressionOperand
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(booleanFormula.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(
                        booleanFormula.ExpectedResult, 
                        formula.Apply(DefaultPropertyHolder, formula.CalculateAggregates(new[] { DefaultPropertyHolder })), 
                        $"Formula text: {formulaTextWithSpaces}, Expression built: {formula}");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyRunIifCalls()
        {
            foreach (var ifFormula in new[] 
            {
                new { FormulaText = "Iif(I != 10, 100 / (I - 10), -500)", Variable = 10, ExpectedResult = (object)-500 },
                new { FormulaText = "Iif(I != 10, 100 / (I - 10), -500)", Variable = 30, ExpectedResult = (object)5 },

                new { FormulaText = "Iif(I != 10, 100 / (I - 10), -500.5)", Variable = 10, ExpectedResult = (object)-500.5M },
                new { FormulaText = "Iif(I != 10, 100 / (I - 10), -500.5)", Variable = 30, ExpectedResult = (object)5M }
            })
            {
                var propertyHolder = new PropertyHolder { I = ifFormula.Variable };
                var formula = _testee.Build(ifFormula.FormulaText);
                Assert.AreEqual(
                    ifFormula.ExpectedResult,
                    formula.Apply(propertyHolder, null),
                    $"Formula text: {ifFormula.FormulaText}, Expression built: {formula}");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseStatementsWithNegation()
        {
            foreach (var formula in new[]
                {
                    new { Text = "|-|1|", ExpectedResult = (object)-1 },
                    new { Text = "|-|[I]|", ExpectedResult = (object)-DefaultPropertyHolder.I },
                    new { Text = "|-|f(|)|", ExpectedResult = (object)-StockFunctions.f() },
                    new { Text = "|-|10|-|5| + 6", ExpectedResult = (object)(-10 - 5 + 6) },
                    new { Text = "|-|(|10|+|f|(|)|)|/|3|", ExpectedResult = (object)(-(10 + StockFunctions.f()) / 3) },
                    new { Text = "|If|(|-|foo|(|1|)|<|0|,|f|(|)|,|-|f|(|)|)|", ExpectedResult = (object)(-StockFunctions.foo(1) < 0 ? StockFunctions.f() : -StockFunctions.f()) }
                })
            {
                foreach (var formulaText in InsertSpacesIntoFormula(formula.Text))
                {
                    var builtFormula = _testee.Build(formulaText);
                    Assert.AreEqual(
                        formula.ExpectedResult,
                        builtFormula.Apply(DefaultPropertyHolder, builtFormula.CalculateAggregates(new[] { DefaultPropertyHolder })),
                        $"Formula text: {formulaText}, Expression built: {builtFormula}");
                }
            }
        }

        private static IEnumerable<string> SurroundWithSpaces(IEnumerable<string> formulaTexts)
        {
            return from formulaText in formulaTexts
                   from sorroundedWithSpaces in new[] { formulaText, $" {formulaText}", $"{formulaText} ", $"\t{formulaText}\t" }
                   select sorroundedWithSpaces;
        }

        private RowFormulaBuilder _testee;

        private static readonly PropertyHolder DefaultPropertyHolder = 
            new PropertyHolder { Book = "default-book", InstSubType = SubType.Regular, I = 42, r = 3.1416M, FXQ_char_45__char_62_U = "Test odd property name" };

#pragma warning disable IDE1006 // Naming Styles
// ReSharper disable InconsistentNaming
        private class PropertyHolder
        {
            public string Book { get; set; }

            public SubType InstSubType { [UsedImplicitly] get; set; }

            public int I { get; set; }

            public decimal r { [UsedImplicitly] get; set; }

            public string FXQ_char_45__char_62_U { [UsedImplicitly] get; set; }

            public static bool TryGetPropertyType(string propertyName, out Type propertyType)
            {
                var propertyInfo = typeof(PropertyHolder).GetProperty(propertyName);
                propertyType = propertyInfo?.PropertyType;
                return propertyInfo != null;
            }
        }
// ReSharper enable InconsistentNaming
#pragma warning restore IDE1006 // Naming Styles

#pragma warning disable IDE1006 // Naming Styles
        // ReSharper disable InconsistentNaming
        private static class StockFunctions
        {
            public static int f()
            {
                return 42;
            }

            public static decimal foo(int v)
            {
                switch (v)
                {
                    case 1:
                        return 3.14M;
                    case 100:
                        return 2.78M;
                    default:
                        throw new ArgumentException("v");
                }
                
            }

            public static string bar(string left, string right)
            {
                return $"{left}=={right}";
            }

            public static string MakeFoo(string p1, decimal p2, DateTime p3)
            {
                return $"{p1}\\/{p2}\\/{CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(p3.Month)}";
            }

            public static bool TryGetFunction(string methodName, out MethodInfo result)
            {
                result = typeof(StockFunctions).GetMethod(methodName);
                return result != null;
            }
        }

        enum SubType { Irregular = 785, Regular = 968 }
    }
// ReSharper enable InconsistentNaming
#pragma warning restore IDE1006 // Naming Styles
}
