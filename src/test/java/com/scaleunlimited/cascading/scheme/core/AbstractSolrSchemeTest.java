package com.scaleunlimited.cascading.scheme.core;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.scheme.local.SolrScheme;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public abstract class AbstractSolrSchemeTest extends Assert {

    private static final String SOLR_HOME_DIR = "src/test/resources/solr-home-4.1/"; 
    protected static final String SOLR_CORE_DIR = SOLR_HOME_DIR + "collection1"; 

    protected abstract String getTestDir();
    
    protected abstract Tap<?, ?, ?> makeSourceTap(Fields fields, String path);
    protected abstract FlowProcess<?> makeFlowProcess();
    protected abstract Tap<?, ?, ?> makeSolrSink(Fields fields, String path) throws Exception;
    protected abstract FlowConnector makeFlowConnector();
    
    protected abstract Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir) throws Exception;
    
    protected abstract Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir, int maxSegments) throws Exception;
    
    protected abstract Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir, int maxSegments, String dataDirPropertyName) throws Exception;
    
    @Before
    public void setup() throws IOException {
        File outputDir = new File(getTestDir());
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    protected void testSchemeChecksMissingConf() throws Exception {
        try {
            makeScheme(new Fields("a", "b"), "bogus-directory");
            fail("Should have thrown exception");
        } catch (Exception e) {
        }
    }

    protected void testSchemeChecksBadConf() throws Exception {
        try {
            makeScheme(new Fields("a", "b"), "src/test/resources");
            fail("Should have thrown exception");
        } catch (TapException e) {
        }
    }
    
    protected void testSchemeWrongFields() throws Exception {
        try {
            // Need to make sure we include the required fields.
            makeScheme(new Fields("id", "bogus-field"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name doesn't exist"));
        }
    }

    protected void testSchemeMissingRequiredField() throws Exception {
        try {
            makeScheme(new Fields("sku"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name for required"));
        }
    }
    
    protected void testIndexSink() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "inStock");
        String out = getTestDir() + "testIndexSink/out";

        DirectoryTap solrSink = new DirectoryTap(new SolrScheme(testFields, SOLR_CORE_DIR), out, SinkMode.REPLACE);
        
        TupleEntryCollector writer = solrSink.openForWrite(new LocalFlowProcess());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple(i, "product #" + i, i * 1.0f, true));
        }

        writer.close();
    }
    
    protected void testSimpleIndexing() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "cat", "inStock", "image");

        final String in = getTestDir() + "testSimpleIndexing/in";
        final String out = getTestDir() + "testSimpleIndexing/out";

        byte[] imageData = new byte[] {0, 1, 2, 3, 5};
        
        Tap source = makeSourceTap(testFields, in);
        TupleEntryCollector write = source.openForWrite(makeFlowProcess());
        Tuple t1 = new Tuple();
        t1.add(1);
        t1.add("TurboWriter 2.3");
        t1.add(395.50f);
        t1.add(new Tuple("wordprocessor", "Japanese"));
        t1.add(true);
        t1.add(imageData);
        write.add(t1);
        
        Tuple t2 = new Tuple();
        t2.isUnmodifiable();
        t2.add(2);
        t2.add("Shasta 1.0");
        t2.add(95.00f);
        t2.add("Chinese");
        t2.add(false);
        
        BytesWritable bw = new BytesWritable(imageData);
        bw.setCapacity(imageData.length + 10);
        t2.add(bw);
        write.add(t2);
        write.close();

        // Now read from the results, and write to a Solr index.
        Pipe writePipe = new Pipe("tuples to Solr");

        Tap solrSink = makeSolrSink(testFields, out);
        Flow flow = makeFlowConnector().connect(source, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        System.setProperty("solr.solr.home", SOLR_HOME_DIR);
        System.setProperty("solr.data.dir", out + "/part-00000");
        CoreContainer coreContainer = new CoreContainer(SOLR_HOME_DIR);
        coreContainer.load();
        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.Q, "turbowriter");

        QueryResponse res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        byte[] storedImageData = (byte[])res.getResults().get(0).getFieldValue("image");
        assertEquals(imageData, storedImageData);
        
        params.set(CommonParams.Q, "cat:Japanese");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "cat:Chinese");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        storedImageData = (byte[])res.getResults().get(0).getFieldValue("image");
        assertEquals(imageData, storedImageData);
        
        params.set(CommonParams.Q, "bogus");
        res = solrServer.query(params);
        assertEquals(0, res.getResults().size());
    }


    private static void assertEquals(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }
    

}
