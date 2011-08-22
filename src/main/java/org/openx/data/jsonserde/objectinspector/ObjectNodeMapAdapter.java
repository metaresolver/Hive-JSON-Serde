package org.openx.data.jsonserde.objectinspector;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ObjectNodeMapAdapter
        extends AbstractMap
{
    private final ObjectNode node;
    private Set<JsonNode> entries;

    public ObjectNodeMapAdapter(ObjectNode node)
    {
        this.node = node;
    }

    @Override
    public int size()
    {
        return node.size();
    }

    @Override
    public boolean isEmpty()
    {
        return node.size() == 0;
    }

    @Override
    public boolean containsValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object put(Object key, Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set keySet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection values()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set entrySet()
    {
        if (entries == null) {
            entries = new HashSet<JsonNode>(node.size());
            for (JsonNode o : node) {
                entries.add(o);
            }
        }
        return entries;
    }
}
