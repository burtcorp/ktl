# encoding: utf-8

module ScalaHelpers
  def scala_list(arr)
    l = Scala::Collection::Mutable::MutableList.empty
    arr.each { |i| l.send('+=', i) }
    l
  end

  def scala_int_list(arr)
    scala_list(arr.map { |i| to_int(i) })
  end

  def to_int(i)
    i.to_java(:Integer)
  end
end
