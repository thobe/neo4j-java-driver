/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.packstream;

import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.exceptions.PackStreamException;

import static java.util.Collections.singletonList;

/**
 * PackStream is a messaging serialisation format heavily inspired by MessagePack.
 * The key differences are in the type system itself which (among other things) replaces extensions with structures.
 * The Packer and Unpacker implementations are also faster than their MessagePack counterparts.
 *
 * Note that several marker byte values are RESERVED for future use.
 * Extra markers should <em>not</em> be added casually and such additions must be follow a strict process involving both client and server software.
 *
 * The table below shows all allocated marker byte values.
 *
 * <table>
 * <tr><th>Marker</th><th>Binary</th><th>Type</th><th>Description</th></tr>
 * <tr><td><code>00..7F</code></td><td><code>0xxxxxxx</code></td><td>+TINY_INT</td><td>Integer 0 to 127</td></tr>
 * <tr><td><code>80..8F</code></td><td><code>1000xxxx</code></td><td>TINY_STRING</td><td></td></tr>
 * <tr><td><code>90..9F</code></td><td><code>1001xxxx</code></td><td>TINY_LIST</td><td></td></tr>
 * <tr><td><code>A0..AF</code></td><td><code>1010xxxx</code></td><td>TINY_MAP</td><td></td></tr>
 * <tr><td><code>B0..BF</code></td><td><code>1011xxxx</code></td><td>TINY_STRUCT</td><td></td></tr>
 * <tr><td><code>C0</code></td><td><code>11000000</code></td><td>NULL</td><td></td></tr>
 * <tr><td><code>C1</code></td><td><code>11000001</code></td><td>FLOAT_64</td><td>64-bit floating point number (double)</td></tr>
 * <tr><td><code>C2</code></td><td><code>11000010</code></td><td>FALSE</td><td>Boolean false</td></tr>
 * <tr><td><code>C3</code></td><td><code>11000011</code></td><td>TRUE</td><td>Boolean true</td></tr>
 * <tr><td><code>C4..C7</code></td><td><code>110001xx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>C8</code></td><td><code>11001000</code></td><td>INT_8</td><td>8-bit signed integer</td></tr>
 * <tr><td><code>C9</code></td><td><code>11001001</code></td><td>INT_8</td><td>16-bit signed integer</td></tr>
 * <tr><td><code>CA</code></td><td><code>11001010</code></td><td>INT_8</td><td>32-bit signed integer</td></tr>
 * <tr><td><code>CB</code></td><td><code>11001011</code></td><td>INT_8</td><td>64-bit signed integer</td></tr>
 * <tr><td><code>CC</code></td><td><code>11001100</code></td><td>BYTES_8</td><td>Byte string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>CD</code></td><td><code>11001101</code></td><td>BYTES_16</td><td>Byte string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>CE</code></td><td><code>11001110</code></td><td>BYTES_32</td><td>Byte string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>CF</code></td><td><code>11001111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D0</code></td><td><code>11010000</code></td><td>STRING_8</td><td>UTF-8 encoded string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>D1</code></td><td><code>11010001</code></td><td>STRING_16</td><td>UTF-8 encoded string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>D2</code></td><td><code>11010010</code></td><td>STRING_32</td><td>UTF-8 encoded string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>D3</code></td><td><code>11010011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D4</code></td><td><code>11010100</code></td><td>LIST_8</td><td>List (fewer than 2<sup>8</sup> items)</td></tr>
 * <tr><td><code>D5</code></td><td><code>11010101</code></td><td>LIST_16</td><td>List (fewer than 2<sup>16</sup> items)</td></tr>
 * <tr><td><code>D6</code></td><td><code>11010110</code></td><td>LIST_32</td><td>List (fewer than 2<sup>32</sup> items)</td></tr>
 * <tr><td><code>D7</code></td><td><code>11010111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D8</code></td><td><code>11011000</code></td><td>MAP_8</td><td>Map (fewer than 2<sup>8</sup> key:value pairs)</td></tr>
 * <tr><td><code>D9</code></td><td><code>11011001</code></td><td>MAP_16</td><td>Map (fewer than 2<sup>16</sup> key:value pairs)</td></tr>
 * <tr><td><code>DA</code></td><td><code>11011010</code></td><td>MAP_32</td><td>Map (fewer than 2<sup>32</sup> key:value pairs)</td></tr>
 * <tr><td><code>DB</code></td><td><code>11011011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>DC</code></td><td><code>11011100</code></td><td>STRUCT_8</td><td>Structure (fewer than 2<sup>8</sup> fields)</td></tr>
 * <tr><td><code>DD</code></td><td><code>11011101</code></td><td>STRUCT_16</td><td>Structure (fewer than 2<sup>16</sup> fields)</td></tr>
 * <tr><td><code>DE</code></td><td><code>11011110</code></td><td>STRUCT_32</td><td>Structure (fewer than 2<sup>32</sup> fields)</td></tr>
 * <tr><td><code>DF</code></td><td><code>11011111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>E0..EF</code></td><td><code>1110xxxx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>F0..FF</code></td><td><code>1111xxxx</code></td><td>-TINY_INT</td><td>Integer -1 to -16</td></tr>
 * </table>
 *
 */
public class PackStream
{

    public static final byte TINY_STRING = (byte) 0x80;
    public static final byte TINY_LIST = (byte) 0x90;
    public static final byte TINY_MAP = (byte) 0xA0;
    public static final byte TINY_STRUCT = (byte) 0xB0;
    public static final byte NULL = (byte) 0xC0;
    public static final byte FLOAT_64 = (byte) 0xC1;
    public static final byte FALSE = (byte) 0xC2;
    public static final byte TRUE = (byte) 0xC3;
    public static final byte RESERVED_C4 = (byte) 0xC4;
    public static final byte RESERVED_C5 = (byte) 0xC5;
    public static final byte RESERVED_C6 = (byte) 0xC6;
    public static final byte RESERVED_C7 = (byte) 0xC7;
    public static final byte INT_8 = (byte) 0xC8;
    public static final byte INT_16 = (byte) 0xC9;
    public static final byte INT_32 = (byte) 0xCA;
    public static final byte INT_64 = (byte) 0xCB;
    public static final byte BYTES_8 = (byte) 0xCC;
    public static final byte BYTES_16 = (byte) 0xCD;
    public static final byte BYTES_32 = (byte) 0xCE;
    public static final byte RESERVED_CF = (byte) 0xCF;
    public static final byte STRING_8 = (byte) 0xD0;
    public static final byte STRING_16 = (byte) 0xD1;
    public static final byte STRING_32 = (byte) 0xD2;
    public static final byte RESERVED_D3 = (byte) 0xD3;
    public static final byte LIST_8 = (byte) 0xD4;
    public static final byte LIST_16 = (byte) 0xD5;
    public static final byte LIST_32 = (byte) 0xD6;
    public static final byte RESERVED_D7 = (byte) 0xD7;
    public static final byte MAP_8 = (byte) 0xD8;
    public static final byte MAP_16 = (byte) 0xD9;
    public static final byte MAP_32 = (byte) 0xDA;
    public static final byte RESERVED_DB = (byte) 0xDB;
    public static final byte STRUCT_8 = (byte) 0xDC;
    public static final byte STRUCT_16 = (byte) 0xDD;
    public static final byte RESERVED_DE = (byte) 0xDE; // TODO STRUCT_32? or the class javadoc is wrong?
    public static final byte RESERVED_DF = (byte) 0xDF;
    public static final byte RESERVED_E0 = (byte) 0xE0;
    public static final byte RESERVED_E1 = (byte) 0xE1;
    public static final byte RESERVED_E2 = (byte) 0xE2;
    public static final byte RESERVED_E3 = (byte) 0xE3;
    public static final byte RESERVED_E4 = (byte) 0xE4;
    public static final byte RESERVED_E5 = (byte) 0xE5;
    public static final byte RESERVED_E6 = (byte) 0xE6;
    public static final byte RESERVED_E7 = (byte) 0xE7;
    public static final byte RESERVED_E8 = (byte) 0xE8;
    public static final byte RESERVED_E9 = (byte) 0xE9;
    public static final byte RESERVED_EA = (byte) 0xEA;
    public static final byte RESERVED_EB = (byte) 0xEB;
    public static final byte RESERVED_EC = (byte) 0xEC;
    public static final byte RESERVED_ED = (byte) 0xED;
    public static final byte RESERVED_EE = (byte) 0xEE;
    public static final byte RESERVED_EF = (byte) 0xEF;

    private static final long PLUS_2_TO_THE_31  = 2147483648L;
    private static final long PLUS_2_TO_THE_16  = 65536L;
    private static final long PLUS_2_TO_THE_15  = 32768L;
    private static final long PLUS_2_TO_THE_7   = 128L;
    private static final long MINUS_2_TO_THE_4  = -16L;
    private static final long MINUS_2_TO_THE_7  = -128L;
    private static final long MINUS_2_TO_THE_15 = -32768L;
    private static final long MINUS_2_TO_THE_31 = -2147483648L;

    private static final String EMPTY_STRING = "";
    private static final Charset UTF_8 = Charset.forName( "UTF-8" );

    private PackStream() {}

    public static class Packer
    {
        private PackOutput out;

        public Packer( PackOutput out )
        {
            this.out = out;
        }

        public void reset( PackOutput out )
        {
            this.out = out;
        }

        public void reset( WritableByteChannel channel )
        {
            ((BufferedChannelOutput) out).reset( channel );
        }

        public void flush() throws PackStreamException.OutputFailure
        {
            out.flush();
        }

        public void packRaw( byte[] data ) throws PackStreamException.OutputFailure
        {
            out.writeBytes( data, 0, data.length );
        }

        public void packNull() throws PackStreamException.OutputFailure
        {
            out.writeByte( NULL );
        }

        public void pack( boolean value ) throws PackStreamException.OutputFailure
        {
            out.writeByte( value ? TRUE : FALSE );
        }

        public void pack( long value ) throws PackStreamException.OutputFailure
        {
            if ( value >= MINUS_2_TO_THE_4 && value < PLUS_2_TO_THE_7)
            {
                out.writeByte( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_7 && value < MINUS_2_TO_THE_4 )
            {
                out.writeByte( INT_8 )
                        .writeByte( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_15 && value < PLUS_2_TO_THE_15 )
            {
                out.writeByte( INT_16 )
                        .writeShort( (short) value );
            }
            else if ( value >= MINUS_2_TO_THE_31 && value < PLUS_2_TO_THE_31 )
            {
                out.writeByte( INT_32 )
                        .writeInt( (int) value );
            }
            else
            {
                out.writeByte( INT_64 )
                        .writeLong( value );
            }
        }

        public void pack( double value ) throws PackStreamException.OutputFailure
        {
            out.writeByte( FLOAT_64 )
                    .writeDouble( value );
        }

        public void pack( byte[] values ) throws PackStreamException.OutputFailure
        {
            if ( values == null ) { packNull(); }
            else
            {
                packBytesHeader( values.length );
                packRaw( values );
            }
        }

        public void pack( String value ) throws PackStreamException.OutputFailure
        {
            if ( value == null ) { packNull(); }
            else
            {
                byte[] utf8 = value.getBytes( UTF_8 );
                packStringHeader( utf8.length );
                packRaw( utf8 );
            }
        }

        public void packString( byte[] utf8 ) throws PackStreamException.OutputFailure
        {
            if ( utf8 == null ) { packNull(); }
            else
            {
                packStringHeader( utf8.length );
                packRaw( utf8 );
            }
        }

        public void pack( List values ) throws PackStreamException.SerializationFailure
        {
            if ( values == null ) { packNull(); }
            else
            {
                packListHeader( values.size() );
                for ( Object value : values )
                {
                    pack( value );
                }
            }
        }

        public void pack( Map values ) throws PackStreamException.SerializationFailure
        {
            if ( values == null ) { packNull(); }
            else
            {
                packMapHeader( values.size() );
                for ( Object key : values.keySet() )
                {
                    pack( key );
                    pack( values.get( key ) );
                }
            }
        }

        public void pack( Object value ) throws PackStreamException.SerializationFailure
        {
            if ( value == null ) { packNull(); }
            else if ( value instanceof Boolean ) { pack( (boolean) value ); }
            else if ( value instanceof boolean[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Byte ) { pack( (byte) value ); }
            else if ( value instanceof byte[] ) { pack( (byte[]) value ); }
            else if ( value instanceof Short ) { pack( (short) value ); }
            else if ( value instanceof short[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Integer ) { pack( (int) value ); }
            else if ( value instanceof int[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Long ) { pack( (long) value ); }
            else if ( value instanceof long[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Float ) { pack( (float) value ); }
            else if ( value instanceof float[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Double ) { pack( (double) value ); }
            else if ( value instanceof double[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof Character ) { pack( Character.toString( (char) value ) ); }
            else if ( value instanceof char[] ) { pack( new String( (char[]) value ) ); }
            else if ( value instanceof String ) { pack( (String) value ); }
            else if ( value instanceof String[] ) { pack( singletonList( value ) ); }
            else if ( value instanceof List ) { pack( (List) value ); }
            else if ( value instanceof Map ) { pack( (Map) value ); }
            else { throw new PackStreamException.Unpackable( value );}
        }

        public void packBytesHeader( int size ) throws PackStreamException.OutputFailure
        {
            if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( BYTES_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < PLUS_2_TO_THE_16 )
            {
                out.writeByte( BYTES_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( BYTES_32 )
                        .writeInt( size );
            }
        }

        public void packStringHeader( int size ) throws PackStreamException.OutputFailure
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_STRING | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( STRING_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < PLUS_2_TO_THE_16 )
            {
                out.writeByte( STRING_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( STRING_32 )
                        .writeInt( size );
            }
        }

        public void packListHeader( int size ) throws PackStreamException.OutputFailure
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_LIST | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( LIST_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < PLUS_2_TO_THE_16 )
            {
                out.writeByte( LIST_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( LIST_32 )
                        .writeInt( size );
            }
        }

        public void packMapHeader( int size ) throws PackStreamException.OutputFailure
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_MAP | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( MAP_8 )
                        .writeByte( (byte) size );
            }
            else if ( size < PLUS_2_TO_THE_16 )
            {
                out.writeByte( MAP_16 )
                        .writeShort( (short) size );
            }
            else
            {
                out.writeByte( MAP_32 )
                        .writeInt( size );
            }
        }

        public void packStructHeader( int size, byte signature ) throws PackStreamException.SerializationFailure
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_STRUCT | size) )
                        .writeByte( signature );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( STRUCT_8 )
                        .writeByte( (byte) size )
                        .writeByte( signature );
            }
            else if ( size < PLUS_2_TO_THE_16 )
            {
                out.writeByte( STRUCT_16 )
                        .writeShort( (short) size )
                        .writeByte( signature );
            }
            else
            {
                throw new PackStreamException.StructureFieldOverflow( size );
            }
        }

    }

    public static class Unpacker
    {
        private PackInput in;

        public Unpacker( PackInput in )
        {
            this.in = in;
        }

        public boolean hasNext() throws PackStreamException.InputFailure
        {
            return in.hasMoreData();
        }

        public long unpackStructHeader() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_STRUCT ) { return markerLowNibble; }
            switch(markerByte)
            {
                case STRUCT_8: return unpackUINT8();
                case STRUCT_16: return unpackUINT16();
                default: throw new PackStreamException.UnexpectedType( markerByte, "struct", STRUCT_8, STRUCT_16 );
            }
        }

        public byte unpackStructSignature() throws PackStreamException.InputFailure
        {
            return in.readByte();
        }

        public long unpackListHeader() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble  = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_LIST ) { return markerLowNibble; }
            switch(markerByte)
            {
                case LIST_8: return unpackUINT8();
                case LIST_16: return unpackUINT16();
                case LIST_32: return unpackUINT32();
                default: throw new PackStreamException.UnexpectedType( markerByte, "list", LIST_8, LIST_16, LIST_32 );
            }
        }

        public long unpackMapHeader() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_MAP ) { return markerLowNibble; }
            switch(markerByte)
            {
                case MAP_8: return unpackUINT8();
                case MAP_16: return unpackUINT16();
                case MAP_32: return unpackUINT32();
                default: throw new PackStreamException.UnexpectedType( markerByte, "map", MAP_8, MAP_16, MAP_32 );
            }
        }

        public long unpackLong() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            if ( markerByte >= MINUS_2_TO_THE_4) { return markerByte; }
            switch(markerByte)
            {
                case INT_8:   return in.readByte();
                case INT_16:  return in.readShort();
                case INT_32:  return in.readInt();
                case INT_64:  return in.readLong();
                default: throw new PackStreamException.UnexpectedType( markerByte, "integer", INT_8, INT_16, INT_32, INT_64 );
            }
        }

        public double unpackDouble() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            if ( markerByte == FLOAT_64 )
            {
                return in.readDouble();
            }
            throw new PackStreamException.UnexpectedType( markerByte, "double", FLOAT_64 );
        }

        public String unpackString() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            if( markerByte == TINY_STRING ) // Note no mask, so we compare to 0x80.
            {
                return EMPTY_STRING;
            }

            return new String(unpackUtf8(markerByte), UTF_8);
        }

        public byte[] unpackBytes() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();

            switch(markerByte)
            {
                case BYTES_8: return unpackBytes( unpackUINT8() );
                case BYTES_16: return unpackBytes( unpackUINT16() );
                case BYTES_32:
                {
                    long size = unpackUINT32();
                    if ( size <= Integer.MAX_VALUE )
                    {
                        return unpackBytes( (int) size );
                    }
                    else
                    {
                        throw new PackStreamException.CannotRepresent( "binary data (BYTES_32)", size );
                    }
                }
                default: throw new PackStreamException.UnexpectedType( markerByte, "binary data", BYTES_8, BYTES_16, BYTES_32 );
            }
        }

        /**
         * This may seem confusing. This method exists to move forward the internal pointer when encountering
         * a null value. The idiomatic usage would be someone using {@link #peekNextType()} to detect a null type,
         * and then this method to "skip past it".
         * @return null
         * @throws PackStreamException.UnexpectedType if the unpacked value was not null
         */
        public Object unpackNull() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            if ( markerByte != NULL )
            {
                throw new PackStreamException.UnexpectedType( markerByte, "null", NULL );
            }
            return null;
        }

        private byte[] unpackUtf8(byte markerByte) throws PackStreamException.DeserializationFailure
        {
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_STRING ) { return unpackBytes( markerLowNibble ); }
            switch(markerByte)
            {
                case STRING_8: return unpackBytes( unpackUINT8() );
                case STRING_16: return unpackBytes( unpackUINT16() );
                case STRING_32:
                {
                    long size = unpackUINT32();
                    if ( size <= Integer.MAX_VALUE )
                    {
                        return unpackBytes( (int) size );
                    }
                    else
                    {
                        throw new PackStreamException.CannotRepresent( "string (STRING_32)", size );
                    }
                }
                default: throw new PackStreamException.UnexpectedType( markerByte, "string", TINY_STRING, STRING_8, STRUCT_16, STRING_32 );
            }
        }

        public boolean unpackBoolean() throws PackStreamException.DeserializationFailure
        {
            final byte markerByte = in.readByte();
            switch ( markerByte )
            {
                case TRUE:
                    return true;
                case FALSE:
                    return false;
                default:
                    throw new PackStreamException.UnexpectedType( markerByte,  "boolean", TRUE, FALSE );
            }
        }

        private int unpackUINT8() throws PackStreamException.InputFailure
        {
            return in.readByte() & 0xFF;
        }

        private int unpackUINT16() throws PackStreamException.InputFailure
        {
            return in.readShort() & 0xFFFF;
        }

        private long unpackUINT32() throws PackStreamException.InputFailure
        {
            return in.readInt() & 0xFFFFFFFFL;
        }

        private byte[] unpackBytes( int size ) throws PackStreamException.InputFailure
        {
            byte[] heapBuffer = new byte[size];
            in.readBytes( heapBuffer, 0, heapBuffer.length );
            return heapBuffer;
        }

        public PackType peekNextType() throws PackStreamException.InputFailure
        {
            final byte markerByte = in.peekByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);

            switch(markerHighNibble)
            {
                case TINY_STRING:   return PackType.STRING;
                case TINY_LIST:   return PackType.LIST;
                case TINY_MAP:    return PackType.MAP;
                case TINY_STRUCT: return PackType.STRUCT;
            }

            switch(markerByte)
            {
                case NULL:
                    return PackType.NULL;
                case TRUE:
                case FALSE:
                    return PackType.BOOLEAN;
                case FLOAT_64:
                    return PackType.FLOAT;
                case BYTES_8:
                case BYTES_16:
                case BYTES_32:
                    return PackType.BYTES;
                case STRING_8:
                case STRING_16:
                case STRING_32:
                    return PackType.STRING;
                case LIST_8:
                case LIST_16:
                case LIST_32:
                    return PackType.LIST;
                case MAP_8:
                case MAP_16:
                case MAP_32:
                    return PackType.MAP;
                case STRUCT_8:
                case STRUCT_16:
                    return PackType.STRUCT;
                default:
                    return PackType.INTEGER;
            }
        }
    }
}
