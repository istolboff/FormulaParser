using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using JetBrains.Annotations;
using static FormulaParser.Tests.FormuaTextUtilities;

namespace FormulaParser.Tests
{
    [TestClass]
    public class AggregationTests
    {
        [TestInitialize]
        public void Setup()
        {
            _testee = new RowFormulaBuilder(
                typeof(DataRow),
                new Dictionary<string, Type> { { "I", typeof(int) }, { "D", typeof(decimal) }, { "S", typeof(string) } }.TryGetValue,
                (string methodName, out MethodInfo result) => (result = GetType().GetMethod(methodName, BindingFlags.Static | BindingFlags.NonPublic)) != null);
        }

        [TestMethod]
        public void TestsForAll()
        {
            var rows = Enumerable.Range(0, 100).Select(i => new DataRow { I = 1000 + i, D = 3.14M * i, S = $"Item-{i}" }).ToArray();
            foreach (var property in new[]
            {
                new { Name = "I", CheckResult = new Predicate<object>(actualResult => rows.Select(row => row.I).SequenceEqual((IEnumerable<int>)actualResult)) },
                new { Name = "D", CheckResult = new Predicate<object>(actualResult => rows.Select(row => row.D).SequenceEqual((IEnumerable<decimal>)actualResult)) },
                new { Name = "S", CheckResult = new Predicate<object>(actualResult => rows.Select(row => row.S).SequenceEqual((IEnumerable<string>)actualResult)) },
                new { Name = "(I + D) / Len(S)", CheckResult = new Predicate<object>(actualResult => rows.Select(row => (row.I + row.D) / row.S.Length).SequenceEqual((IEnumerable<decimal>)actualResult)) }
            })
            {
                foreach (var formulaText in InsertSpacesIntoFormula($"|[|all|:|{property.Name}|]|"))
                {
                    var builtFormula = _testee.Build(formulaText);
                    var calculatedAggregates = builtFormula.CalculateAggregates(rows);
                    for (var i = 0; i != rows.Length; ++i)
                    {
                        Assert.IsTrue(property.CheckResult(builtFormula.Apply(rows[i], calculatedAggregates)), $"[{formulaText}] failed for {i}");
                    }
                }
            }
        }

        [TestMethod]
        public void TestFirstAndLast()
        {
            var rows = Enumerable.Range(0, 100).Select(i => new DataRow { I = 1000 + i, D = 3.14M * i, S = $"Item-{i}" }).ToArray();
            decimal Calculate(DataRow row) => (row.I + row.D) / row.S.Length;
            foreach (var property in new[]
            {
                new { Name = "I", First = (object)rows.First().I, Last = (object)rows.Last().I },
                new { Name = "D", First = (object)rows.First().D, Last = (object)rows.Last().D },
                new { Name = "S", First = (object)rows.First().S, Last = (object)rows.Last().S },
                new { Name = "(I + D) / Len(S)", First = (object)Calculate(rows.First()), Last = (object)Calculate(rows.Last()) }
            })
            {
                foreach (var aggregationMethod in new[] { AggregationMethod.first, AggregationMethod.last })
                    foreach (var formulaText in InsertSpacesIntoFormula($"|[|{aggregationMethod}|:|{property.Name}|]|"))
                    {
                        var builtFormula = _testee.Build(formulaText);
                        var calculatedAggregates = builtFormula.CalculateAggregates(rows);
                        for (var i = 0; i != rows.Length; ++i)
                        {
                            Assert.AreEqual(
                                aggregationMethod == AggregationMethod.first ? property.First : property.Last,
                                builtFormula.Apply(rows[i], calculatedAggregates),
                                $"[{formulaText}] failed for {i}");
                        }
                    }
            }
        }

        [TestMethod]
        public void TestCachingAggregatedEnumerables()
        {
            var rows = Enumerable.Range(0, 100).Select(i => new DataRow { I = 1000 + i, D = 3.14M * i, S = $"Item-{i}" }).ToArray();
            foreach (var formula in new[]
            {
                new { Text = "([first: CountInvocations(D)] + [last:CountInvocations(D)]) / 2", ExpectedInvocationCount = rows.Length, ExpectedResult = (object)((CountInvocations(rows.First().D) + CountInvocations(rows.Last().D)) / 2) },
            })
            {
                MethodCountInvocations = 0;
                var builtFormula = _testee.Build(formula.Text);
                var calculatedAggregates = builtFormula.CalculateAggregates(rows);
                Assert.AreEqual(
                    formula.ExpectedResult, 
                    builtFormula.Apply(rows[21], calculatedAggregates),
                    $"Failed formula: {formula.Text}");
                Assert.AreEqual(formula.ExpectedInvocationCount, MethodCountInvocations, $"Failed formula: {formula.Text}");
            }
        }

        [TestMethod, Ignore]
        public void DoNotForgetToCheckTheseCases()
        {
            Assert.Fail("[FX Q->U] + [first:FX Q->U]");
            Assert.Fail("[last:r] / [FX Q->U]");
            Assert.Fail("r + I * [first:r] + [last:I]"); // !!!!!
            Assert.Fail("If(len([FX Q->U]) > sum([all:r + len([FX Q->U])]), max([all:I], r)");
            Assert.Fail("sum([all: (I + r) / 100])");
            Assert.Fail("max([all: If(I < r, Book, [FX Q->U])])");
            Assert.Fail("min([all: something]) + min(a, b)"); // overloading function used for aggregated and non-aggregated data
        }

        [TestMethod, Ignore]
        public void DoNotcall_ToArray_In_CompileAggregatedValuesCalculators_TryToCastTo_IReadOnlyCollection_First()
        {
            Assert.Fail();
        }

        [TestMethod, Ignore]
        public void ThenItShouldCorrectlyParseComplexFormulas()
        {
            foreach (var formulaData in new[]
                {
                    new
                    {
                        FormulaText =
@"
if ( [Prod] = 'MM',
	if(  ([Quantity] =0) || (sum ([all:Quantity]) = [first:Quantity]) ,
		0,
		-sum( [all:Quantity] ) / (max([Days],1) * [first:Quantity] /365 )
	),
	if ( [vsBase] = 'USD',
		if ( [Instrument] = 'USD', 0 , [Swap Rate] ),
		if ( [vsBase] = 'RUB',
			if ( [Instrument] = 'RUB',   [Wgt Rate RUB std] , 
				( (1+[Wgt Rate RUB std]/365) * (1+[Swap Rate]/365) - 1) * 365 
			),
			if ( [vsBase] = 'EUR',
				if ( [Instrument] = 'EUR',   [Wgt Rate EUR std] , 
					( (1+[Wgt Rate EUR std]/365) * (1+[Swap Rate]/365) - 1) * 365 
				),
				999
			)
		)
	)
",
                        ExpectedResult = (object)0
                    }
                })
            {
                var formula = _testee.Build(formulaData.FormulaText);
                Assert.AreEqual(
                    formulaData.ExpectedResult,
                    formula.Apply(new DataRow { }, null),
                    $"Formula text: {formulaData.FormulaText}, Expression built: {formula}");

            }
        }

        [UsedImplicitly]
        private static int Len(string v) => v.Length;

        [UsedImplicitly]
        private static int CountInvocations(decimal unused)
        {
            ++MethodCountInvocations;
            return (int)unused;
        }

        private RowFormulaBuilder _testee;

        private static int MethodCountInvocations;

        private class DataRow
        {
            public int I { get; set; }

            public decimal D { get; set; }

            public string S { get; set; }
        }
    }
}
