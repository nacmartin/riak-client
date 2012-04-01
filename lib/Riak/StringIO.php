<?php

namespace Riak;

/**
 * Private class used to accumulate a CURL response.
 * @package StringIO
 */
class StringIO {
    public function __construct() {
        $this->contents = '';
    }

    public function write($ch, $data) {
        $this->contents .= $data;
        return strlen($data);
    }

    public function contents() {
        return $this->contents;
    }
}
