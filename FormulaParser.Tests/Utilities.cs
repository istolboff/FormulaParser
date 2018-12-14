using System;
using System.Collections.Generic;
using System.Linq;

namespace FormulaParser.Tests
{
    public static class CollectionExtensions
    {
        public static IReadOnlyCollection<T> Concatenate<T>(T head, IReadOnlyCollection<T> tail)
        {
            if (tail is IList<T> list)
            {
                list.Insert(0, head);
                return tail is IReadOnlyCollection<T> result ? result : list.ToList();
            }

            var newList = new List<T>(1 + tail.Count) { head };
            newList.AddRange(tail);
            return newList;
        }
    }

    public readonly struct Maybe<T>
    {
        public Maybe(T value)
        {
            _hasValue = true;
            _value = value;
        }

        public Maybe<TOther> Map<TOther>(Func<T, TOther> map)
        {
            return _hasValue ? new Maybe<TOther>(map(_value)) : new Maybe<TOther>();
        }

        public T OrElse(T defaultValue)
        {
            return _hasValue ? _value : defaultValue;
        }

        public override string ToString()
        {
            return _hasValue ? _value.ToString() : $"Maybe<{typeof(T).Name}>.None";
        }

        public static implicit operator Maybe<T>(MaybeNoneFactory noneFactory) => new Maybe<T>();

        private readonly bool _hasValue;
        private readonly T _value;
    }

    public static class Maybe
    {
        public static Maybe<T> Some<T>(T value)
        {
            return new Maybe<T>(value);
        }

        public static MaybeNoneFactory None => new MaybeNoneFactory();
    };

    public struct MaybeNoneFactory
    {
    }

    public struct Either<TLeft, TRight>
    {
        public Either(TLeft left, TRight right, bool isLeft)
            : this()
        {
            _left = left;
            _right = right;
            _isLeft = isLeft;
        }

        public TResult Fold<TResult>(Func<TLeft, TResult> getFromLeft, Func<TRight, TResult> getFromRight) =>
            _isLeft ? getFromLeft(_left) : getFromRight(_right);

        public void Fold(Action<TLeft> processLeft, Action<TRight> processRight)
        {
            if (_isLeft)
            {
                processLeft(_left);
            }
            else
            {
                processRight(_right);
            }
        }

        public Either<TLeft, TValue2> SelectMany<TIntermediate, TValue2>(
            Func<TRight, Either<TLeft, TIntermediate>> selector,
            Func<TRight, TIntermediate, TValue2> projector)
        {
            if (_isLeft)
            {
                return new Either<TLeft, TValue2>(_left, default, true);
            }

            var intermediate = selector(_right);
            if (intermediate._isLeft)
            {
                return new Either<TLeft, TValue2>(intermediate._left, default, true);
            }

            return new Either<TLeft, TValue2>(default, projector(_right, intermediate._right), false);
        }

        public override string ToString()
        {
            return Fold(l => l.ToString(), r => r.ToString());
        }

        public static implicit operator Either<TLeft, TRight>(EitherLeftFactory<TLeft> leftFactory) =>
            new Either<TLeft, TRight>(leftFactory.Left, default, true);

        public static implicit operator Either<TLeft, TRight>(EitherRightFactory<TRight> rightFactory) =>
            new Either<TLeft, TRight>(default, rightFactory.Right, false);

        // Immutable fields would prevent Either from being de-serialized an a different AppDomain.
        // ReSharper disable FieldCanBeMadeReadOnly.Local
        private bool _isLeft;
        private TLeft _left;
        private TRight _right;
        // ReSharper enable FieldCanBeMadeReadOnly.Local
    }

    internal static class Either
    {
        public static Either<TLeft, TRight> Left<TLeft, TRight>(TLeft left) =>
            new Either<TLeft, TRight>(left, default, true);

        public static Either<TLeft, TRight> Right<TLeft, TRight>(TRight right) =>
            new Either<TLeft, TRight>(default, right, false);

        public static EitherLeftFactory<TLeft> Left<TLeft>(TLeft left) =>
            new EitherLeftFactory<TLeft>(left);

        public static EitherRightFactory<TRight> Right<TRight>(TRight right) =>
            new EitherRightFactory<TRight>(right);

        public static Either<TLeft, TResult> Map<TLeft, TRight, TResult>(
            this Either<TLeft, TRight> @this,
            Func<TRight, TResult> getFromRight) =>
            @this.Fold(Left<TLeft, TResult>, right => Right<TLeft, TResult>(getFromRight(right)));

        public static Either<TLeft, TResult> Select<TLeft, TRight, TResult>(
            this Either<TLeft, TRight> @this,
            Func<TRight, TResult> getFromRight) =>
            @this.Fold(Left<TLeft, TResult>, right => Right<TLeft, TResult>(getFromRight(right)));

        public static Either<TLeft, TResult> FlatMap<TLeft, TRight, TResult>(
            this Either<TLeft, TRight> @this,
            Func<TRight, Either<TLeft, TResult>> getFromRight) =>
            @this.Fold(Left<TLeft, TResult>, getFromRight);
    }

    public readonly struct EitherLeftFactory<TLeft>
    {
        public EitherLeftFactory(TLeft left)
            : this()
        {
            Left = left;
        }

        public readonly TLeft Left;
    }

    public readonly struct EitherRightFactory<TRight>
    {
        public EitherRightFactory(TRight right)
            : this()
        {
            Right = right;
        }

        public readonly TRight Right;
    }
}
