package com.github.marschall.jfr.jdbctemplate;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import jdk.jfr.Event;

final class JfrEventStream<T> implements Stream<T> {

  private final Stream<T> delegate;

  private final Event event;

  JfrEventStream(Stream<T> delegate, Event event) {
    this.delegate = delegate;
    this.event = event;
  }

  @Override
  public Iterator<T> iterator() {
    return this.delegate.iterator();
  }

  @Override
  public Spliterator<T> spliterator() {
    return this.delegate.spliterator();
  }

  @Override
  public boolean isParallel() {
    return this.delegate.isParallel();
  }

  @Override
  public Stream<T> sequential() {
    return this.delegate.sequential();
  }

  @Override
  public Stream<T> parallel() {
    return this.delegate.parallel();
  }

  @Override
  public Stream<T> unordered() {
    return this.delegate.unordered();
  }

  @Override
  public Stream<T> onClose(Runnable closeHandler) {
    return this.delegate.onClose(closeHandler);
  }

  @Override
  public void close() {
    this.event.end();
    this.event.commit();
    this.delegate.close();
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    return this.delegate.filter(predicate);
  }

  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    return this.delegate.map(mapper);
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    return this.delegate.mapToInt(mapper);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    return this.delegate.mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return this.delegate.mapToDouble(mapper);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return this.delegate.flatMap(mapper);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    return this.delegate.flatMapToInt(mapper);
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    return this.delegate.flatMapToLong(mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    return this.delegate.flatMapToDouble(mapper);
  }

  @Override
  public Stream<T> distinct() {
    return this.delegate.distinct();
  }

  @Override
  public Stream<T> sorted() {
    return this.delegate.sorted();
  }

  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    return this.delegate.sorted(comparator);
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) {
    return this.delegate.peek(action);
  }

  @Override
  public Stream<T> limit(long maxSize) {
    return this.delegate.limit(maxSize);
  }

  @Override
  public Stream<T> skip(long n) {
    return this.delegate.skip(n);
  }

  @Override
  public Stream<T> takeWhile(Predicate<? super T> predicate) {
    return this.delegate.takeWhile(predicate);
  }

  @Override
  public Stream<T> dropWhile(Predicate<? super T> predicate) {
    return this.delegate.dropWhile(predicate);
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    this.delegate.forEach(action);
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    this.delegate.forEachOrdered(action);
  }

  @Override
  public Object[] toArray() {
    return this.delegate.toArray();
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return this.delegate.toArray(generator);
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    return this.delegate.reduce(identity, accumulator);
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    return this.delegate.reduce(accumulator);
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    return this.delegate.reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    return this.delegate.collect(supplier, accumulator, combiner);
  }

  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return this.delegate.collect(collector);
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return this.delegate.min(comparator);
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return this.delegate.max(comparator);
  }

  @Override
  public long count() {
    return this.delegate.count();
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return this.delegate.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return this.delegate.allMatch(predicate);
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    return this.delegate.noneMatch(predicate);
  }

  @Override
  public Optional<T> findFirst() {
    return this.delegate.findFirst();
  }

  @Override
  public Optional<T> findAny() {
    return this.delegate.findAny();
  }

}