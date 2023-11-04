<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/protobuf/descriptor.proto

namespace Google\Protobuf\Internal;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\GPBWire;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\InputStream;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>google.protobuf.MessageOptions</code>
 */
class MessageOptions extends \Google\Protobuf\Internal\Message
{
    /**
     * Set true to use the old proto1 MessageSet wire format for extensions.
     * This is provided for backwards-compatibility with the MessageSet wire
     * format.  You should not use this for any other reason:  It's less
     * efficient, has fewer features, and is more complicated.
     * The message must be defined exactly as follows:
     *   message Foo {
     *     option message_set_wire_format = true;
     *     extensions 4 to max;
     *   }
     * Note that the message cannot have any defined fields; MessageSets only
     * have extensions.
     * All extensions of your type must be singular messages; e.g. they cannot
     * be int32s, enums, or repeated messages.
     * Because this is an option, the above two restrictions are not enforced by
     * the protocol compiler.
     *
     * Generated from protobuf field <code>optional bool message_set_wire_format = 1 [default = false];</code>
     */
    protected $message_set_wire_format = null;
    /**
     * Disables the generation of the standard "descriptor()" accessor, which can
     * conflict with a field of the same name.  This is meant to make migration
     * from proto1 easier; new code should avoid fields named "descriptor".
     *
     * Generated from protobuf field <code>optional bool no_standard_descriptor_accessor = 2 [default = false];</code>
     */
    protected $no_standard_descriptor_accessor = null;
    /**
     * Is this message deprecated?
     * Depending on the target platform, this can emit Deprecated annotations
     * for the message, or it will be completely ignored; in the very least,
     * this is a formalization for deprecating messages.
     *
     * Generated from protobuf field <code>optional bool deprecated = 3 [default = false];</code>
     */
    protected $deprecated = null;
    /**
     * NOTE: Do not set the option in .proto files. Always use the maps syntax
     * instead. The option should only be implicitly set by the proto compiler
     * parser.
     * Whether the message is an automatically generated map entry type for the
     * maps field.
     * For maps fields:
     *     map<KeyType, ValueType> map_field = 1;
     * The parsed descriptor looks like:
     *     message MapFieldEntry {
     *         option map_entry = true;
     *         optional KeyType key = 1;
     *         optional ValueType value = 2;
     *     }
     *     repeated MapFieldEntry map_field = 1;
     * Implementations may choose not to generate the map_entry=true message, but
     * use a native map in the target language to hold the keys and values.
     * The reflection APIs in such implementations still need to work as
     * if the field is a repeated message field.
     *
     * Generated from protobuf field <code>optional bool map_entry = 7;</code>
     */
    protected $map_entry = null;
    /**
     * Enable the legacy handling of JSON field name conflicts.  This lowercases
     * and strips underscored from the fields before comparison in proto3 only.
     * The new behavior takes `json_name` into account and applies to proto2 as
     * well.
     * This should only be used as a temporary measure against broken builds due
     * to the change in behavior for JSON field name conflicts.
     * TODO This is legacy behavior we plan to remove once downstream
     * teams have had time to migrate.
     *
     * Generated from protobuf field <code>optional bool deprecated_legacy_json_field_conflicts = 11 [deprecated = true];</code>
     * @deprecated
     */
    protected $deprecated_legacy_json_field_conflicts = null;
    /**
     * Any features defined in the specific edition.
     *
     * Generated from protobuf field <code>optional .google.protobuf.FeatureSet features = 12;</code>
     */
    protected $features = null;
    /**
     * The parser stores options it doesn't recognize here. See above.
     *
     * Generated from protobuf field <code>repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;</code>
     */
    private $uninterpreted_option;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type bool $message_set_wire_format
     *           Set true to use the old proto1 MessageSet wire format for extensions.
     *           This is provided for backwards-compatibility with the MessageSet wire
     *           format.  You should not use this for any other reason:  It's less
     *           efficient, has fewer features, and is more complicated.
     *           The message must be defined exactly as follows:
     *             message Foo {
     *               option message_set_wire_format = true;
     *               extensions 4 to max;
     *             }
     *           Note that the message cannot have any defined fields; MessageSets only
     *           have extensions.
     *           All extensions of your type must be singular messages; e.g. they cannot
     *           be int32s, enums, or repeated messages.
     *           Because this is an option, the above two restrictions are not enforced by
     *           the protocol compiler.
     *     @type bool $no_standard_descriptor_accessor
     *           Disables the generation of the standard "descriptor()" accessor, which can
     *           conflict with a field of the same name.  This is meant to make migration
     *           from proto1 easier; new code should avoid fields named "descriptor".
     *     @type bool $deprecated
     *           Is this message deprecated?
     *           Depending on the target platform, this can emit Deprecated annotations
     *           for the message, or it will be completely ignored; in the very least,
     *           this is a formalization for deprecating messages.
     *     @type bool $map_entry
     *           NOTE: Do not set the option in .proto files. Always use the maps syntax
     *           instead. The option should only be implicitly set by the proto compiler
     *           parser.
     *           Whether the message is an automatically generated map entry type for the
     *           maps field.
     *           For maps fields:
     *               map<KeyType, ValueType> map_field = 1;
     *           The parsed descriptor looks like:
     *               message MapFieldEntry {
     *                   option map_entry = true;
     *                   optional KeyType key = 1;
     *                   optional ValueType value = 2;
     *               }
     *               repeated MapFieldEntry map_field = 1;
     *           Implementations may choose not to generate the map_entry=true message, but
     *           use a native map in the target language to hold the keys and values.
     *           The reflection APIs in such implementations still need to work as
     *           if the field is a repeated message field.
     *     @type bool $deprecated_legacy_json_field_conflicts
     *           Enable the legacy handling of JSON field name conflicts.  This lowercases
     *           and strips underscored from the fields before comparison in proto3 only.
     *           The new behavior takes `json_name` into account and applies to proto2 as
     *           well.
     *           This should only be used as a temporary measure against broken builds due
     *           to the change in behavior for JSON field name conflicts.
     *           TODO This is legacy behavior we plan to remove once downstream
     *           teams have had time to migrate.
     *     @type \Google\Protobuf\Internal\FeatureSet $features
     *           Any features defined in the specific edition.
     *     @type array<\Google\Protobuf\Internal\UninterpretedOption>|\Google\Protobuf\Internal\RepeatedField $uninterpreted_option
     *           The parser stores options it doesn't recognize here. See above.
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Google\Protobuf\Internal\Descriptor::initOnce();
        parent::__construct($data);
    }

    /**
     * Set true to use the old proto1 MessageSet wire format for extensions.
     * This is provided for backwards-compatibility with the MessageSet wire
     * format.  You should not use this for any other reason:  It's less
     * efficient, has fewer features, and is more complicated.
     * The message must be defined exactly as follows:
     *   message Foo {
     *     option message_set_wire_format = true;
     *     extensions 4 to max;
     *   }
     * Note that the message cannot have any defined fields; MessageSets only
     * have extensions.
     * All extensions of your type must be singular messages; e.g. they cannot
     * be int32s, enums, or repeated messages.
     * Because this is an option, the above two restrictions are not enforced by
     * the protocol compiler.
     *
     * Generated from protobuf field <code>optional bool message_set_wire_format = 1 [default = false];</code>
     * @return bool
     */
    public function getMessageSetWireFormat()
    {
        return isset($this->message_set_wire_format) ? $this->message_set_wire_format : false;
    }

    public function hasMessageSetWireFormat()
    {
        return isset($this->message_set_wire_format);
    }

    public function clearMessageSetWireFormat()
    {
        unset($this->message_set_wire_format);
    }

    /**
     * Set true to use the old proto1 MessageSet wire format for extensions.
     * This is provided for backwards-compatibility with the MessageSet wire
     * format.  You should not use this for any other reason:  It's less
     * efficient, has fewer features, and is more complicated.
     * The message must be defined exactly as follows:
     *   message Foo {
     *     option message_set_wire_format = true;
     *     extensions 4 to max;
     *   }
     * Note that the message cannot have any defined fields; MessageSets only
     * have extensions.
     * All extensions of your type must be singular messages; e.g. they cannot
     * be int32s, enums, or repeated messages.
     * Because this is an option, the above two restrictions are not enforced by
     * the protocol compiler.
     *
     * Generated from protobuf field <code>optional bool message_set_wire_format = 1 [default = false];</code>
     * @param bool $var
     * @return $this
     */
    public function setMessageSetWireFormat($var)
    {
        GPBUtil::checkBool($var);
        $this->message_set_wire_format = $var;

        return $this;
    }

    /**
     * Disables the generation of the standard "descriptor()" accessor, which can
     * conflict with a field of the same name.  This is meant to make migration
     * from proto1 easier; new code should avoid fields named "descriptor".
     *
     * Generated from protobuf field <code>optional bool no_standard_descriptor_accessor = 2 [default = false];</code>
     * @return bool
     */
    public function getNoStandardDescriptorAccessor()
    {
        return isset($this->no_standard_descriptor_accessor) ? $this->no_standard_descriptor_accessor : false;
    }

    public function hasNoStandardDescriptorAccessor()
    {
        return isset($this->no_standard_descriptor_accessor);
    }

    public function clearNoStandardDescriptorAccessor()
    {
        unset($this->no_standard_descriptor_accessor);
    }

    /**
     * Disables the generation of the standard "descriptor()" accessor, which can
     * conflict with a field of the same name.  This is meant to make migration
     * from proto1 easier; new code should avoid fields named "descriptor".
     *
     * Generated from protobuf field <code>optional bool no_standard_descriptor_accessor = 2 [default = false];</code>
     * @param bool $var
     * @return $this
     */
    public function setNoStandardDescriptorAccessor($var)
    {
        GPBUtil::checkBool($var);
        $this->no_standard_descriptor_accessor = $var;

        return $this;
    }

    /**
     * Is this message deprecated?
     * Depending on the target platform, this can emit Deprecated annotations
     * for the message, or it will be completely ignored; in the very least,
     * this is a formalization for deprecating messages.
     *
     * Generated from protobuf field <code>optional bool deprecated = 3 [default = false];</code>
     * @return bool
     */
    public function getDeprecated()
    {
        return isset($this->deprecated) ? $this->deprecated : false;
    }

    public function hasDeprecated()
    {
        return isset($this->deprecated);
    }

    public function clearDeprecated()
    {
        unset($this->deprecated);
    }

    /**
     * Is this message deprecated?
     * Depending on the target platform, this can emit Deprecated annotations
     * for the message, or it will be completely ignored; in the very least,
     * this is a formalization for deprecating messages.
     *
     * Generated from protobuf field <code>optional bool deprecated = 3 [default = false];</code>
     * @param bool $var
     * @return $this
     */
    public function setDeprecated($var)
    {
        GPBUtil::checkBool($var);
        $this->deprecated = $var;

        return $this;
    }

    /**
     * NOTE: Do not set the option in .proto files. Always use the maps syntax
     * instead. The option should only be implicitly set by the proto compiler
     * parser.
     * Whether the message is an automatically generated map entry type for the
     * maps field.
     * For maps fields:
     *     map<KeyType, ValueType> map_field = 1;
     * The parsed descriptor looks like:
     *     message MapFieldEntry {
     *         option map_entry = true;
     *         optional KeyType key = 1;
     *         optional ValueType value = 2;
     *     }
     *     repeated MapFieldEntry map_field = 1;
     * Implementations may choose not to generate the map_entry=true message, but
     * use a native map in the target language to hold the keys and values.
     * The reflection APIs in such implementations still need to work as
     * if the field is a repeated message field.
     *
     * Generated from protobuf field <code>optional bool map_entry = 7;</code>
     * @return bool
     */
    public function getMapEntry()
    {
        return isset($this->map_entry) ? $this->map_entry : false;
    }

    public function hasMapEntry()
    {
        return isset($this->map_entry);
    }

    public function clearMapEntry()
    {
        unset($this->map_entry);
    }

    /**
     * NOTE: Do not set the option in .proto files. Always use the maps syntax
     * instead. The option should only be implicitly set by the proto compiler
     * parser.
     * Whether the message is an automatically generated map entry type for the
     * maps field.
     * For maps fields:
     *     map<KeyType, ValueType> map_field = 1;
     * The parsed descriptor looks like:
     *     message MapFieldEntry {
     *         option map_entry = true;
     *         optional KeyType key = 1;
     *         optional ValueType value = 2;
     *     }
     *     repeated MapFieldEntry map_field = 1;
     * Implementations may choose not to generate the map_entry=true message, but
     * use a native map in the target language to hold the keys and values.
     * The reflection APIs in such implementations still need to work as
     * if the field is a repeated message field.
     *
     * Generated from protobuf field <code>optional bool map_entry = 7;</code>
     * @param bool $var
     * @return $this
     */
    public function setMapEntry($var)
    {
        GPBUtil::checkBool($var);
        $this->map_entry = $var;

        return $this;
    }

    /**
     * Enable the legacy handling of JSON field name conflicts.  This lowercases
     * and strips underscored from the fields before comparison in proto3 only.
     * The new behavior takes `json_name` into account and applies to proto2 as
     * well.
     * This should only be used as a temporary measure against broken builds due
     * to the change in behavior for JSON field name conflicts.
     * TODO This is legacy behavior we plan to remove once downstream
     * teams have had time to migrate.
     *
     * Generated from protobuf field <code>optional bool deprecated_legacy_json_field_conflicts = 11 [deprecated = true];</code>
     * @return bool
     * @deprecated
     */
    public function getDeprecatedLegacyJsonFieldConflicts()
    {
        @trigger_error('deprecated_legacy_json_field_conflicts is deprecated.', E_USER_DEPRECATED);
        return isset($this->deprecated_legacy_json_field_conflicts) ? $this->deprecated_legacy_json_field_conflicts : false;
    }

    public function hasDeprecatedLegacyJsonFieldConflicts()
    {
        @trigger_error('deprecated_legacy_json_field_conflicts is deprecated.', E_USER_DEPRECATED);
        return isset($this->deprecated_legacy_json_field_conflicts);
    }

    public function clearDeprecatedLegacyJsonFieldConflicts()
    {
        @trigger_error('deprecated_legacy_json_field_conflicts is deprecated.', E_USER_DEPRECATED);
        unset($this->deprecated_legacy_json_field_conflicts);
    }

    /**
     * Enable the legacy handling of JSON field name conflicts.  This lowercases
     * and strips underscored from the fields before comparison in proto3 only.
     * The new behavior takes `json_name` into account and applies to proto2 as
     * well.
     * This should only be used as a temporary measure against broken builds due
     * to the change in behavior for JSON field name conflicts.
     * TODO This is legacy behavior we plan to remove once downstream
     * teams have had time to migrate.
     *
     * Generated from protobuf field <code>optional bool deprecated_legacy_json_field_conflicts = 11 [deprecated = true];</code>
     * @param bool $var
     * @return $this
     * @deprecated
     */
    public function setDeprecatedLegacyJsonFieldConflicts($var)
    {
        @trigger_error('deprecated_legacy_json_field_conflicts is deprecated.', E_USER_DEPRECATED);
        GPBUtil::checkBool($var);
        $this->deprecated_legacy_json_field_conflicts = $var;

        return $this;
    }

    /**
     * Any features defined in the specific edition.
     *
     * Generated from protobuf field <code>optional .google.protobuf.FeatureSet features = 12;</code>
     * @return \Google\Protobuf\Internal\FeatureSet|null
     */
    public function getFeatures()
    {
        return $this->features;
    }

    public function hasFeatures()
    {
        return isset($this->features);
    }

    public function clearFeatures()
    {
        unset($this->features);
    }

    /**
     * Any features defined in the specific edition.
     *
     * Generated from protobuf field <code>optional .google.protobuf.FeatureSet features = 12;</code>
     * @param \Google\Protobuf\Internal\FeatureSet $var
     * @return $this
     */
    public function setFeatures($var)
    {
        GPBUtil::checkMessage($var, \Google\Protobuf\Internal\FeatureSet::class);
        $this->features = $var;

        return $this;
    }

    /**
     * The parser stores options it doesn't recognize here. See above.
     *
     * Generated from protobuf field <code>repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getUninterpretedOption()
    {
        return $this->uninterpreted_option;
    }

    /**
     * The parser stores options it doesn't recognize here. See above.
     *
     * Generated from protobuf field <code>repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;</code>
     * @param array<\Google\Protobuf\Internal\UninterpretedOption>|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setUninterpretedOption($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::MESSAGE, \Google\Protobuf\Internal\UninterpretedOption::class);
        $this->uninterpreted_option = $arr;

        return $this;
    }

}

