package com.data.writer;

import com.clicktale.data.PageViewRecord;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * Created by Gennady.Gilin on 6/30/2016.
 */

/**
 * A {@link Writer} that uses {@code toString()} on the input elements and writes them to
 * the output bucket file separated by newline.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
public class PageViewEventsWriter<T> implements Writer<T> {
    private static final long serialVersionUID = 1L;

    private transient FSDataOutputStream outputStream;

    private String charsetName;

    private transient Charset charset;

    /**
     * Creates a new {@code StringWriter} that uses {@code "UTF-8"} charset to convert
     * strings to bytes.
     */
    public PageViewEventsWriter() {
        this("UTF-8");
    }

    /**
     * Creates a new {@code StringWriter} that uses the given charset to convert
     * strings to bytes.
     *
     * @param charsetName Name of the charset to be used, must be valid input for {@code Charset.forName(charsetName)}
     */
    public PageViewEventsWriter( String charsetName ) {
        this.charsetName = charsetName;
    }

    @Override
    public void open( FSDataOutputStream outStream) throws IOException {
        if ( outputStream != null) {

            throw new IllegalStateException("StringWriter has already been opened.");
        }
        this.outputStream = outStream;

        try {
            this.charset = Charset.forName(charsetName);
        }
        catch (IllegalCharsetNameException e) {
            throw new IOException("The charset " + charsetName + " is not valid.", e);
        }
        catch (UnsupportedCharsetException e) {
            throw new IOException("The charset " + charsetName + " is not supported.", e);
        }
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
        outputStream = null;
    }

    @Override
    public void write( T element) throws IOException {
        if (outputStream == null) {
            throw new IllegalStateException("StringWriter has not been opened.");
        }
        outputStream.write( ((PageViewRecord) element).toEventsString().getBytes( charset ) );
       // outputStream.write('\n');

    }

    @Override
    public Writer<T> duplicate() {
        return new PageViewEventsWriter<>();
    }
}
