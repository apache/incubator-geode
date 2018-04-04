/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal;

import org.apache.geode.DataSerializer;

/**
 * An interface that contains a bunch of static final values used for the implementation of
 * {@link DataSerializer}. It is basically an Enum and could be changed to one once we drop 1.4. The
 * allowed range of these codes is -128..127 inclusive (i.e. byte).
 *
 * @since GemFire 5.7
 * @deprecated Use {@link HeaderByte} instead.
 */
@Deprecated
public interface DSCODE {
  /**
   * This byte value, -128, has never been used in any GemFire release so far. It might get used in
   * the future to introduce DataSerializer versioning.
   */
  byte RESERVED_FOR_FUTURE_USE = HeaderByte.RESERVED_FOR_FUTURE_USE.toByte();

  byte ILLEGAL = HeaderByte.ILLEGAL.toByte();

  // -126..0 unused

  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed byte.
   *
   * @since GemFire 5.7
   */
  byte DS_FIXED_ID_BYTE = HeaderByte.DS_FIXED_ID_BYTE.toByte();
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed short.
   *
   * @since GemFire 5.7
   */
  byte DS_FIXED_ID_SHORT = HeaderByte.DS_FIXED_ID_SHORT.toByte();
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is a single signed int.
   *
   * @since GemFire 5.7
   */
  byte DS_FIXED_ID_INT = HeaderByte.DS_FIXED_ID_INT.toByte();
  /**
   * A header byte meaning that the next element in the stream is a {@link DataSerializableFixedID}
   * whose id is <code>NO_FIXED_ID</code>.
   *
   * @since GemFire 5.7
   */
  byte DS_NO_FIXED_ID = HeaderByte.DS_NO_FIXED_ID.toByte();

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>
   * and the id is encoded with 2 bytes.
   *
   * @since GemFire 5.7
   */
  byte USER_CLASS_2 = HeaderByte.USER_CLASS_2.toByte();

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>
   * and the id is encoded with 4 bytes.
   *
   * @since GemFire 5.7
   */
  byte USER_CLASS_4 = HeaderByte.USER_CLASS_4.toByte();

  // TypeIds 7 and 8 reserved for use by C# Serializable and XmlSerializable.

  // 9 unused

  /**
   * A header byte meaning that the next element in the stream is a <code>LinkedList</code>.
   */
  byte LINKED_LIST = HeaderByte.LINKED_LIST.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Properties</code>.
   *
   * @since GemFire 5.7
   */
  byte PROPERTIES = HeaderByte.PROPERTIES.toByte();

  // 12..16 unused

  /**
   * Codes for the primitive classes, which cannot be recreated with
   * <code>Class.forName(String,boolean,ClassLoader)</code> like other classes
   */
  byte BOOLEAN_TYPE = HeaderByte.BOOLEAN_TYPE.toByte();
  byte CHARACTER_TYPE = HeaderByte.CHARACTER_TYPE.toByte();
  byte BYTE_TYPE = HeaderByte.BYTE_TYPE.toByte();
  byte SHORT_TYPE = HeaderByte.SHORT_TYPE.toByte();
  byte INTEGER_TYPE = HeaderByte.INTEGER_TYPE.toByte();
  byte LONG_TYPE = HeaderByte.LONG_TYPE.toByte();
  byte FLOAT_TYPE = HeaderByte.FLOAT_TYPE.toByte();
  byte DOUBLE_TYPE = HeaderByte.DOUBLE_TYPE.toByte();
  byte VOID_TYPE = HeaderByte.VOID_TYPE.toByte();

  /**
   * @since GemFire 5.7
   */
  byte BOOLEAN_ARRAY = HeaderByte.BOOLEAN_ARRAY.toByte();
  /**
   * @since GemFire 5.7
   */
  byte CHAR_ARRAY = HeaderByte.CHAR_ARRAY.toByte();

  // 28..36 unused

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using four bytes for its ID.
   *
   * @since GemFire 5.7
   */
  byte USER_DATA_SERIALIZABLE_4 = HeaderByte.USER_DATA_SERIALIZABLE_4.toByte();

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using two bytes for its ID.
   *
   * @since GemFire 5.7
   */
  byte USER_DATA_SERIALIZABLE_2 = HeaderByte.USER_DATA_SERIALIZABLE_2.toByte();

  /**
   * A header byte meaning that a DataSerializable that was registered with the Instantiator was
   * data serialized using a single byte for its ID.
   */
  byte USER_DATA_SERIALIZABLE = HeaderByte.USER_DATA_SERIALIZABLE.toByte();

  /**
   * A header byte meaning that the object was serialized by a user's <code>DataSerializer</code>.
   */
  byte USER_CLASS = HeaderByte.USER_CLASS.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>null</code>
   */
  byte NULL = HeaderByte.NULL.toByte();

  /**
   * A header byte meaning that the next element in the stream is a String
   */
  byte STRING = HeaderByte.STRING.toByte();

  /**
   * A header byte meaning that the next element in the stream is a (non-primitive) Class
   */
  byte CLASS = HeaderByte.CLASS.toByte();

  /**
   * A header byte meaning that the next element in the stream is a serialized object
   */
  byte SERIALIZABLE = HeaderByte.SERIALIZABLE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a DataSerializable object
   */
  byte DATA_SERIALIZABLE = HeaderByte.DATA_SERIALIZABLE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>byte</code> array.
   */
  byte BYTE_ARRAY = HeaderByte.BYTE_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>short</code> array.
   */
  byte SHORT_ARRAY = HeaderByte.SHORT_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>int</code> array.
   */
  byte INT_ARRAY = HeaderByte.INT_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>long</code> array.
   */
  byte LONG_ARRAY = HeaderByte.LONG_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>float</code> array.
   */
  byte FLOAT_ARRAY = HeaderByte.FLOAT_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>double</code> array.
   */
  byte DOUBLE_ARRAY = HeaderByte.DOUBLE_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Object</code> array.
   */
  byte OBJECT_ARRAY = HeaderByte.OBJECT_ARRAY.toByte();

  /**
   * A header boolean meaning that the next element in the stream is a <code>Boolean</code>.
   */
  byte BOOLEAN = HeaderByte.BOOLEAN.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Character</code>.
   */
  byte CHARACTER = HeaderByte.CHARACTER.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Byte</code>.
   */
  byte BYTE = HeaderByte.BYTE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Short</code>.
   */
  byte SHORT = HeaderByte.SHORT.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Integer</code>.
   */
  byte INTEGER = HeaderByte.INTEGER.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Long</code>.
   */
  byte LONG = HeaderByte.LONG.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Float</code>.
   */
  byte FLOAT = HeaderByte.FLOAT.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Double</code>.
   */
  byte DOUBLE = HeaderByte.DOUBLE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Date</code>.
   */
  byte DATE = HeaderByte.DATE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>InetAddress</code>.
   */
  byte INET_ADDRESS = HeaderByte.INET_ADDRESS.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>File</code>.
   */
  byte FILE = HeaderByte.FILE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>String</code> array.
   */
  byte STRING_ARRAY = HeaderByte.STRING_ARRAY.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>ArrayList</code>.
   */
  byte ARRAY_LIST = HeaderByte.ARRAY_LIST.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>HashSet</code>.
   */
  byte HASH_SET = HeaderByte.HASH_SET.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>HashMap</code>.
   */
  byte HASH_MAP = HeaderByte.HASH_MAP.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>TimeUnit</code>.
   */
  byte TIME_UNIT = HeaderByte.TIME_UNIT.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>null</code>
   * <code>String</code>.
   */
  byte NULL_STRING = HeaderByte.NULL_STRING.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Hashtable</code>.
   *
   * @since GemFire 5.7
   */
  byte HASH_TABLE = HeaderByte.HASH_TABLE.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Vector</code>.
   *
   * @since GemFire 5.7
   */
  byte VECTOR = HeaderByte.VECTOR.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>IdentityHashMap</code>.
   *
   * @since GemFire 5.7
   */
  byte IDENTITY_HASH_MAP = HeaderByte.IDENTITY_HASH_MAP.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>LinkedHashSet</code>.
   *
   * @since GemFire 5.7
   */
  byte LINKED_HASH_SET = HeaderByte.LINKED_HASH_SET.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>Stack</code>.
   *
   * @since GemFire 5.7
   */
  byte STACK = HeaderByte.STACK.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>TreeMap</code>.
   *
   * @since GemFire 5.7
   */
  byte TREE_MAP = HeaderByte.TREE_MAP.toByte();

  /**
   * A header byte meaning that the next element in the stream is a <code>TreeSet</code>.
   *
   * @since GemFire 5.7
   */
  byte TREE_SET = HeaderByte.TREE_SET.toByte();

  // 75..86 unused

  /**
   * A header byte meaning that the next element in the stream is a buffer of 1-byte characters to
   * turn into a String whose length is <= 0xFFFF
   */
  byte STRING_BYTES = HeaderByte.STRING_BYTES.toByte();

  /**
   * A header byte meaning that the next element in the stream is a buffer of 1-byte characters to
   * turn into a String whose length is > 0xFFFF.
   *
   * @since GemFire 5.7
   */
  byte HUGE_STRING_BYTES = HeaderByte.HUGE_STRING_BYTES.toByte();

  /**
   * A header byte meaning that the next element in the stream is a buffer of 2-byte characters to
   * turn into a String whose length is > 0xFFFF.
   *
   * @since GemFire 5.7
   */
  byte HUGE_STRING = HeaderByte.HUGE_STRING.toByte();

  // 90 unused

  /**
   * A header byte meaning that the next element in the stream is a <code>byte[][]</code>.
   */
  byte ARRAY_OF_BYTE_ARRAYS = HeaderByte.ARRAY_OF_BYTE_ARRAYS.toByte();

  // 92 unused

  /**
   * A header byte meaning that the next element in the stream is a PdxSerializable object.
   *
   * @since GemFire 6.6
   */
  byte PDX = HeaderByte.PDX.toByte();

  /**
   * A header byte meaning that the next element in the stream is an enum whose type is defined in
   * the pdx registry.
   *
   * @since GemFire 6.6.2
   */
  byte PDX_ENUM = HeaderByte.PDX_ENUM.toByte();

  /**
   * java.math.BigInteger
   *
   * @since GemFire 6.6.2
   */
  byte BIG_INTEGER = HeaderByte.BIG_INTEGER.toByte();
  /**
   * java.math.BigDecimal
   *
   * @since GemFire 6.6.2
   */
  byte BIG_DECIMAL = HeaderByte.BIG_DECIMAL.toByte();

  /**
   * This code can only be used by PDX. It can't be used for normal DataSerializer writeObject
   * because it would break backward compatibility. A header byte meaning that the next element in
   * the stream is a ConcurrentHashMap object.
   *
   * @since GemFire 6.6
   */
  byte CONCURRENT_HASH_MAP = HeaderByte.CONCURRENT_HASH_MAP.toByte();

  /**
   * java.util.UUID
   *
   * @since GemFire 6.6.2
   */
  byte UUID = HeaderByte.UUID.toByte();
  /**
   * java.sql.Timestamp
   *
   * @since GemFire 6.6.2
   */
  byte TIMESTAMP = HeaderByte.TIMESTAMP.toByte();

  /**
   * Used for enums that need to always be deserialized into their enum domain class.
   *
   * @since GemFire 6.6.2
   */
  byte GEMFIRE_ENUM = HeaderByte.GEMFIRE_ENUM.toByte();
  /**
   * Used for enums that need to be encoded inline because the pdx registry may not be available.
   * During deserialization this type of enum may be deserialized as a PdxInstance.
   *
   * @since GemFire 6.6.2
   */
  byte PDX_INLINE_ENUM = HeaderByte.PDX_INLINE_ENUM.toByte();

  /**
   * Used for wildcard searches in soplogs with composite keys.
   *
   * @since GemFire 8.0
   */
  byte WILDCARD = HeaderByte.WILDCARD.toByte();

  // 103..127 unused

  // DO NOT USE CODES > 127. They are not "byte".
}
