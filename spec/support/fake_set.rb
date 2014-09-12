# encoding: utf-8

class FakeSet < Struct.new(:items)
  def contains?(id)
    items.include?(id)
  end

  def index_of(id)
    items.index(id)
  end

  def updated(index, id, implicit)
    items[index] = id
    items
  end
end
