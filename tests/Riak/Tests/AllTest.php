<?php

namespace Riak\Tests;

use Riak\Bucket;
use Riak\Client;
use Riak\LinkPhase;
use Riak\Link;
use Riak\MapReducePhase;
use Riak\MapReduce;
use Riak\Object;
use Riak\StringIO;
use Riak\Utils;

define('$GLOBALS['RIAK_PORT']', 8091);

class AllTest extends \PHPUnit_Framework_TestCase
{
    public function testIsAlive() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $this->AssertTrue($client->isAlive());
    }

    public function testStoreAndGet() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject('foo', $rand);
        $obj->store();

        $obj = $bucket->get('foo');
        $this->AssertTrue($obj->exists());
        $this->AssertEquals('bucket', $obj->getBucket()->getName());
        $this->AssertEquals('foo', $obj->getKey());
        $this->AssertEquals($rand, $obj->getData());
    }

    public function testStoreAndGetWithoutKey() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject(null, $rand);
        $obj->store();

        $key = $obj->key;

        $obj = $bucket->get($key);
        $this->assertTrue($obj->exists());
        $this->assertEquals("bucket", $obj->getBucket()->getName());
        $this->assertEquals($key, $obj->getKey());
        $this->assertEquals($rand, $obj->getData());
    }

    public function testBinaryStoreAndGet() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');

        # Store as binary, retrieve as binary, then compare...
        $rand = rand();
        $obj = $bucket->newBinary('foo1', $rand);
        $obj->store();
        $obj = $bucket->getBinary('foo1');
        $this->assertTrue($obj->exists());
        $this->assertEquals($rand, $obj->getData());

        # Store as JSON, retrieve as binary, JSON-decode, then compare...
        $data = array(rand(), rand(), rand());
        $obj = $bucket->newObject('foo2', $data);
        $obj->store();
        $obj = $bucket->getBinary('foo2');
        $this->assertEquals($data, json_decode($obj->getData()));
    }

    public function testMissingObject() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');
        $obj = $bucket->get("missing");
        $this->assertFalse($obj->exists());
        $this->assertNull($obj->getData());
    }

    public function testDelete() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject('foo', $rand);
        $obj->store();

        $obj = $bucket->get('foo');
        $this->assertTrue($obj->exists());

        $obj->delete();
        $obj->reload();
        $this->assertFalse($obj->exists());
    }

    public function testSetBucketProperties() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('bucket');

        # Test setting allow mult...
        $bucket->setAllowMultiples(TRUE);
        $this->assertTrue($bucket->getAllowMultiples());

        # Test setting nval...
        $bucket->setNVal(3);
        $this->assertEquals(3, $bucket->getNVal());

        # Test setting multiple properties...
        $bucket->setProperties(array("allow_mult"=>FALSE, "n_val"=>2));
        $this->assertTrue(!$bucket->getAllowMultiples());
        $this->assertEquals(2, $bucket->getNVal());
    }

    public function testSiblings() {
        # Set up the bucket, clear any existing object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket('multiBucket');
        $bucket->setAllowMultiples('true');
        $obj = $bucket->get('foo');
        $obj->delete();

        # Store the same object multiple times...
        for ($i=0; $i<5; $i++) {
            $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
            $bucket = $client->bucket('multiBucket');
            $obj = $bucket->newObject('foo', rand());
            $obj->store();
        }

        # Make sure the object has 5 siblings...
        $this->assertTrue($obj->hasSiblings());
        $this->assertEquals(5, $obj->getSiblingCount());

        # Test getSibling()/getSiblings()...
        $siblings = $obj->getSiblings();
        $obj3 = $obj->getSibling(3);
        $this->assertEquals($siblings[3]->getData(), $obj3->getData());

        # Resolve the conflict, and then do a get...
        $obj3 = $obj->getSibling(3);
        $obj3->store();

        $obj->reload();
        $this->assertEquals($obj->getData(), $obj3->getData());

        # Clean up for next test...
        $obj->delete();
    }

    public function testJavascriptSourceMap() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo")->
            map("function (v) { return [JSON.parse(v.values[0].data)]; }") ->
            run();
        $this->assertEquals(array(2), $result);
    }

    public function testJavascriptNamedMap() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo")->
            map("Riak.mapValuesJson") ->
            run();
        $this->assertEquals(array(2), $result);
    }

    public function testJavascriptSourceMapReduce() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map("function (v) { return [1]; }") ->
            reduce("Riak.reduceSum")->
            run();
        $this->assertEquals(3, $result[0]);
    }

    public function testJavascriptNamedMapReduce() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map("Riak.mapValuesJson") ->
            reduce("Riak.reduceSum")->
            run();
        $this->assertEquals(array(9), $result);
    }

    function testJavascriptBucketMapReduce() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket_" . rand());
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $client->
            add($bucket->name)->
            map("Riak.mapValuesJson") ->
            reduce("Riak.reduceSum")->
            run();
        $this->assertEquals(array(9), $result);
    }

    function testJavascriptArgMapReduce() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo", 5)->
            add("bucket", "foo", 10)->
            add("bucket", "foo", 15)->
            add("bucket", "foo", -15)->
            add("bucket", "foo", -5)->
            map("function(v, arg) { return [arg]; }")->
            reduce("Riak.reduceSum")->
            run();
        $this->assertEquals(array(10), $result);
    }

    function testErlangMapReduce() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 2)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map(array("riak_kv_mapreduce", "map_object_value")) ->
            reduce(array("riak_kv_mapreduce", "reduce_set_union"))->
            run();
        $this->assertCount(2, $result);
    }

    function testMapReduceFromObject() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        $obj = $bucket->get("foo");
        $result = $obj->map("Riak.mapValuesJson")->run();
        $this->assertEquals(array(2), $result);
    }

    function testKeyFilter() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("filter_bucket");
        $bucket->newObject("foo_one",   array("foo"=>"one"  ))->store();
        $bucket->newObject("foo_two",   array("foo"=>"two"  ))->store();
        $bucket->newObject("foo_three", array("foo"=>"three"))->store();
        $bucket->newObject("foo_four",  array("foo"=>"four" ))->store();
        $bucket->newObject("moo_five",  array("foo"=>"five" ))->store();

        $mapred = $client
            ->add($bucket->name)
            ->key_filter(array('tokenize', '_', 1), array('eq', 'foo'));
        $results = $mapred->run();
        $this->assertCount(4, $results);
    }

    function testKeyFilterOperator() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("filter_bucket");
        $bucket->newObject("foo_one",   array("foo"=>"one"  ))->store();
        $bucket->newObject("foo_two",   array("foo"=>"two"  ))->store();
        $bucket->newObject("foo_three", array("foo"=>"three"))->store();
        $bucket->newObject("foo_four",  array("foo"=>"four" ))->store();
        $bucket->newObject("moo_five",  array("foo"=>"five" ))->store();

        $mapred = $client
            ->add($bucket->name)
            ->key_filter(array('starts_with', 'foo'))
            ->key_filter_or(array('ends_with', 'five'));
        $results = $mapred->run();
        $this->assertCount(5, $results);
    }

    function testStoreAndGetLinks() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->
            addLink($bucket->newObject("foo1"))->
            addLink($bucket->newObject("foo2"), "tag")->
            addLink($bucket->newObject("foo3"), "tag2!@#$%^&*")->
            store();

        $obj = $bucket->get("foo");
        $links = $obj->getLinks();
        $this->assertCount(3, $links);
    }

    public function testLinkWalking() {
        # Create the object...
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("bucket");
        $bucket->newObject("foo", 2)->
            addLink($bucket->newObject("foo1", "test1")->store())->
            addLink($bucket->newObject("foo2", "test2")->store(), "tag")->
            addLink($bucket->newObject("foo3", "test3")->store(), "tag2!@#$%^&*")->
            store();

        $obj = $bucket->get("foo");
        $results = $obj->link("bucket")->run();
        $this->assertCount(3, $results);

        $results = $obj->link("bucket", "tag")->run();
        $this->assertCount(1, $results);
    }

    //TODO: fix test that fails also in riak-php-client
    //function testSearchIntegration() {
    //    # Create some objects to search across...
    //    $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
    //    $bucket = $client->bucket("searchbucket");
    //    $bucket->newObject("one", array("foo"=>"one", "bar"=>"red"))->store();
    //    $bucket->newObject("two", array("foo"=>"two", "bar"=>"green"))->store();
    //    $bucket->newObject("three", array("foo"=>"three", "bar"=>"blue"))->store();
    //    $bucket->newObject("four", array("foo"=>"four", "bar"=>"orange"))->store();
    //    $bucket->newObject("five", array("foo"=>"five", "bar"=>"yellow"))->store();

    //    # Run some operations...
    //    $results = $client->search("searchbucket", "foo:one OR foo:two")->run();
    //    if (count($results) == 0) {
    //        print "\n\nNot running test \"testSearchIntegration()\".\n";
    //        print "Please ensure that you have installed the Riak Search hook on bucket \"searchbucket\" by running \"bin/search-cmd install searchbucket\".\n\n";
    //        return;
    //    }
    //    $this->assertCount(2, $results);

    //    $results = $client->search("searchbucket", "(foo:one OR foo:two OR foo:three OR foo:four) AND (NOT bar:green)")->run();
    //    $this->assertCount(3, $results);
    //}

    public function testSecondaryIndexes() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("indextest");

        # Immediate test to see if 2i is even supported w/ the backend
        try {
            $bucket->indexSearch("foo", "bar_bin", "baz");
        }
        catch (\Exception $e) {
            if (strpos($e->__toString(), "indexes_not_supported") !== FALSE) {
                return true;
            } else {
                throw $e;
            }
        }

        # Okay, continue with the rest of the test
        $bucket
            ->newObject("one", array("foo"=>1, "bar"=>"red"))
            ->addIndex("number", "int", 1)
            ->addIndex("text", "bin", "apple")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("two", array("foo"=>2, "bar"=>"green"))
            ->addIndex("number", "int", 2)
            ->addIndex("text", "bin", "avocado")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("three", array("foo"=>3, "bar"=>"blue"))
            ->addIndex("number", "int", 3)
            ->addIndex("text", "bin", "blueberry")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("four", array("foo"=>4, "bar"=>"orange"))
            ->addIndex("number", "int", 4)
            ->addIndex("text", "bin", "citrus")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("five", array("foo"=>5, "bar"=>"yellow"))
            ->addIndex("number", "int", 5)
            ->addIndex("text", "bin", "banana")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();

        $bucket
            ->newObject("six", array("foo"=>6, "bar"=>"purple"))
            ->addIndex("number", "int", 6)
            ->addIndex("number", "int", 7)
            ->addIndex("number", "int", 8)
            ->setIndex("text", "bin", array("x","y","z"))
            ->store();

        # Exact matches
        $results = $bucket->indexSearch("number", "int", 5);
        $this->assertCount(1, $results);

        $results = $bucket->indexSearch("text", "bin", "apple");
        $this->assertCount(1, $results);

        # Range searches
        $results = $bucket->indexSearch("foo", "int", 1, 3);
        $this->assertCount(1, $results);

        $results = $bucket->indexSearch("bar", "bin", "blue", "orange");
        $this->assertCount(1, $results);

        # Test duplicate key de-duping
        $results = $bucket->indexSearch("number", "int", 6, 8, true);
        $this->assertCount(1, $results);

        $results = $bucket->indexSearch("text", "bin", "x", "z", true);
        $this->assertCount(1, $results);

        # Test auto indexes don't leave cruft indexes behind, and regular
        # indexes are preserved
        $object = $bucket->get("one");
        $object->setData(array("foo"=>9, "bar"=>"plaid"));
        $object->store();

        # Auto index updates
        $results = $bucket->indexSearch("foo", "int", 9);
        $this->assertCount(1, $results);

        # Auto index leaves no cruft
        $results = $bucket->indexSearch("foo", "int", 1);
        $this->assertCount(1, $results);

        # Normal index is preserved
        $results = $bucket->indexSearch("number", "int", 1);
        $this->assertCount(1, $results);


        # Test proper collision handling on autoIndex and regular index on same field
        $bucket
            ->newObject("seven", array("foo"=>7))
            ->addAutoIndex("foo", "int")
            ->addIndex("foo", "int", 7)
            ->store();

        $results = $bucket->indexSearch("foo", "int", 7);
        $this->assertCount(1, $results);

        $object = $bucket->get("seven");
        $object->setData(array("foo"=>8));
        $object->store();

        $results = $bucket->indexSearch("foo", "int", 8);
        $this->assertCount(1, $results);

        $results = $bucket->indexSearch("foo", "int", 7);
        $this->assertCount(1, $results);

    }

    public function testMetaData() {
        $client = new Client($GLOBALS['RIAK_HOST'], $GLOBALS['RIAK_PORT']);
        $bucket = $client->bucket("metatest");

        # Set some meta
        $bucket->newObject("metatest", array("foo"=>'bar'))
            ->setMeta("foo", "bar")->store();

        # Test that we load the meta back
        $object = $bucket->get("metatest");
        $this->assertEquals("bar", $object->getMeta("foo"));

        # Test that the meta is preserved when we rewrite the object
        $bucket->get("metatest")->store();
        $object = $bucket->get("metatest");
        $this->assertEquals("bar", $object->getMeta("foo"));

        # Test that we remove meta
        $object->removeMeta("foo")->store();
        $anotherObject = $bucket->get("metatest");
        $this->assertNull($anotherObject->getMeta("foo"));
    }
}
