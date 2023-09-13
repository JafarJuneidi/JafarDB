package org.jafardb;

import java.util.Arrays;

public record Item(byte[] key, byte[] value) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Item item = (Item) o;
        return Arrays.equals(key, item.key) && Arrays.equals(value, item.value);
    }
}
