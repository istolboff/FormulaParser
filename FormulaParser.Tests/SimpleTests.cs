using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Globalization;

namespace FormulaParser.Tests
{
    [TestClass]
    public class SimpleTests
    {
        [TestInitialize]
        public void Setup()
        {
            _testee = new FormulaBuilder(typeof(PropertyHolder), PropertyHolder.TryGetPropertyType, StockFunctions.TryGetFunction);
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseIntegerLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "0", "1", "-1", (int.MinValue + 1).ToString(), int.MaxValue.ToString() }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(int.Parse(formulaText), (int)formula.Apply(), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseDecimalLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "1.0", "-1.0", decimal.MinValue.ToString(), decimal.MaxValue.ToString() }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(decimal.Parse(formulaText), (decimal)formula.Apply(), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseDateTimeLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "'2018-12-10 10:47:03Z'", "'2018-12-10 10:47:03'" }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(DateTime.Parse(formulaText.Trim(' ', '\t', '\'')), (DateTime)formula.Apply(), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParseStringLiterals()
        {
            foreach (var formulaText in SurroundWithSpaces(new[] { "", " ", @"\'", @"   \'     ", "f", "Ö", "hello!", @"f-w\'ggh\'ffjfjfjf\'ggg" }))
            {
                var formula = _testee.Build($"'{formulaText}'");
                Assert.AreEqual(formulaText.Replace(@"\'", "'"), (string)formula.Apply(), $"Formula text: [{formulaText}]");
            }
        }

        [TestMethod]
        public void ThenItShouldCorrectlyParsePropertyAccessors()
        {
            var propertyHolder = new PropertyHolder { Book = Guid.NewGuid().ToString(), InstSubType = SubType.Regular, I = 42, r = 3.1416M };
            foreach (var formulaText in SurroundWithSpaces(new[] { "Book", "[InstSubType]", "I", "[r]" }))
            {
                var formula = _testee.Build(formulaText);
                Assert.AreEqual(
                    propertyHolder.GetType().GetProperty(formulaText.Trim('[', ' ', ']', '\t')).GetValue(propertyHolder), 
                    formula.Apply(propertyHolder), 
                    $"Formula text: [{formulaText}]");
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
                        formula.Apply(DefaultPropertyHolder),
                        $"Formula text: [{formulaTextWithSpaces}]");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsForInvalidFunctionCalls()
        {
            foreach (var functionCall in new[]
                {
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
                        var formula = _testee.Build(formulaTextWithSpaces);
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

            Assert.Fail(@"Check the following cases: 
* If(division-by-zero) special case.");
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
                    Assert.AreEqual(invocation.ExpectedResult, formula.Apply(DefaultPropertyHolder), $"Formula text: {formulaTextWithSpaces}");
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
                    Assert.AreEqual(invocation.ExpectedResult, formula.Apply(DefaultPropertyHolder), $"Formula text: {formulaTextWithSpaces}");
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
                        var formula = _testee.Build(formulaTextWithSpaces);
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
                    Assert.AreEqual(comparisons.ExpectedResult, formula.Apply(DefaultPropertyHolder), $"Formula text: {formulaTextWithSpaces}");
                }
            }
        }

        [TestMethod]
        public void ThenItShouldProduceCorrectParsingErrorsWhenUsingNonBooleanValuesInBooleanExpressions()
        {
            foreach (var booleanFormula in new[]
                {
                    new { FormulaText = "|1|~or~|3|>|5|", ExpectedExceptionText = "fooo" },
                    new { FormulaText = "|8|&&|3.0|==|3|", ExpectedExceptionText = "fooo" },
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(booleanFormula.FormulaText))
                {
                    try
                    {
                        var formula = _testee.Build(formulaTextWithSpaces);
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
                    new { FormulaText = "|1|<|3|~or~|3|>|5|", ExpectedResult = (object)(1 < 3 || 3 > 5) },
                    new { FormulaText = "|8|-|3|<=|9|-|5|&&|3.0|==|3|", ExpectedResult = (object)(8 - 3 <= 9 - 5 && 3M == 3) },
                })
            {
                foreach (var formulaTextWithSpaces in InsertSpacesIntoFormula(booleanFormula.FormulaText))
                {
                    var formula = _testee.Build(formulaTextWithSpaces);
                    Assert.AreEqual(booleanFormula.ExpectedResult, formula.Apply(DefaultPropertyHolder), $"Formula text: {formulaTextWithSpaces}");
                }
            }
        }

        private static IEnumerable<string> SurroundWithSpaces(IEnumerable<string> formulaTexts)
        {
            return from formulaText in formulaTexts
                   from sorroundedWithSpaces in new[] { formulaText, $" {formulaText}", $"{formulaText} ", $"\t{formulaText}\t" }
                   select sorroundedWithSpaces;
        }

        private static IEnumerable<string> InsertSpacesIntoFormula(string formulaText)
        {
            foreach (var space in new[] { string.Empty, " ", "\t" })
            {
                yield return string.Join(space, formulaText.Split('|')).Replace("~or~", "||");
            }
        }

        private FormulaBuilder _testee;

        private static readonly PropertyHolder DefaultPropertyHolder = 
            new PropertyHolder { Book = "default-book", InstSubType = SubType.Regular, I = 42, r = 3.1416M };

        private class PropertyHolder
        {
            public string Book { get; set; }
            public SubType InstSubType { get; set; }
            public int I { get; set; }
            #pragma warning disable IDE1006 // Naming Styles
            public decimal r { get; set; }
            #pragma warning restore IDE1006 // Naming Styles

            public static bool TryGetPropertyType(string propertyName, out Type propertyType)
            {
                var propertyInfo = typeof(PropertyHolder).GetProperty(propertyName);
                propertyType = propertyInfo?.PropertyType;
                return propertyInfo != null;
            }
        }

        private static class StockFunctions
        {
            #pragma warning disable IDE1006 // Naming Styles
            public static int f()
            #pragma warning restore IDE1006 // Naming Styles
            {
                return 42;
            }

            #pragma warning disable IDE1006 // Naming Styles
            public static decimal foo(int v)
            #pragma warning restore IDE1006 // Naming Styles
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

            #pragma warning disable IDE1006 // Naming Styles
            public static string bar(string left, string right)
            #pragma warning restore IDE1006 // Naming Styles
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
}
