using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FormulaParser.Tests
{
    [TestClass]
    public sealed class GivenReadOnlyList
    {
        [TestMethod]
        public void AndItisEmptyThenItShouldProduceEmptyEnumeration()
        {
            Assert.IsTrue(Enumerable.Empty<int>().SequenceEqual(ReadOnlyList<int>.Nil));
            Assert.IsTrue(Enumerable.Empty<int?>().SequenceEqual(ReadOnlyList<int?>.Nil));
            Assert.IsTrue(Enumerable.Empty<string>().SequenceEqual(ReadOnlyList<string>.Nil));
        }

        [TestMethod]
        public void AndItContainsSingleElementThenItShouldProduceCorrectEnumeration()
        {
            foreach (var value in new[] { 0, -1, 1, int.MinValue, int.MaxValue })
            {
                Assert.IsTrue(new[] { value }.SequenceEqual(ReadOnlyList.Create(value)));
            }

            foreach (var value in new int?[] { null, 0, -1, 1, int.MinValue, int.MaxValue })
            {
                Assert.IsTrue(new[] { value }.SequenceEqual(ReadOnlyList.Create(value)));
            }

            foreach (var value in new [] { null, string.Empty, "0" })
            {
                Assert.IsTrue(new[] { value }.SequenceEqual(ReadOnlyList.Create(value)));
            }
        }

        [TestMethod]
        public void AndItIsSimpleChainOfRegularNodesThenItShouldProduceCorrectEnumeration()
        {
            var elements = Enumerable.Range(0, 100);
            Assert.IsTrue(elements.Reverse().SequenceEqual(elements.Aggregate(ReadOnlyList<int>.Nil, (list, item) => list.PushFront(item))));
            Assert.IsTrue(elements.Reverse().SequenceEqual(elements.Aggregate(ReadOnlyList<int>.Nil, (list, item) => ReadOnlyList.Create(item).Append(list))));
        }

        [TestMethod]
        public void AndItIsCombinationOfTwoListsThenItShouldProduceCorrectEnumeration()
        {
            Assert.IsTrue(Enumerable.Empty<int>().SequenceEqual(ReadOnlyList<int>.Nil.Append(ReadOnlyList<int>.Nil)));

            Assert.IsTrue(Enumerable.Repeat(100, 1).SequenceEqual(ReadOnlyList<int>.Nil.Append(ReadOnlyList.Create(100))));
            Assert.IsTrue(Enumerable.Repeat(100, 1).SequenceEqual(ReadOnlyList.Create(100).Append(ReadOnlyList<int>.Nil)));

            Assert.IsTrue(new[] { 101, 202, 303, 404 }.SequenceEqual(MakeList(101, 202).Append(MakeList(303, 404))));

            var elements = Enumerable.Range(0, 100);
            Assert.IsTrue(elements.SequenceEqual(elements.Aggregate(ReadOnlyList<int>.Nil, (list, item) => list.Append(ReadOnlyList.Create(item)))));

            Assert.IsTrue(new[] { 101, 202, 303, 404, 505, 606, 707, 808 }
                            .SequenceEqual(
                                MakeList(101, 202).Append(MakeList(303, 404))
                                .Append(
                                    MakeList(505, 606).Append(MakeList(707, 808)))));
        }

        private static ReadOnlyList<T> MakeList<T>(params T[] items)
        {
            return items.Reverse().Aggregate(ReadOnlyList<T>.Nil, (list, item) => list.PushFront(item));
        }
    }
}
