#include <benchmark/benchmark.h>

#include <string.h>
#ifdef __X86_64__
#include <x86intrin.h>
#endif

#include <string>
#include <random>

#include "google/protobuf/parse_context.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/generated_message_tctable_impl.h"
#include "google/protobuf/json/json.h"

#include "benchmarks/test.pb.h"
#include "benchmarks/test.upb.h"

ABSL_ATTRIBUTE_NOINLINE
void QuitMsg [[noreturn]] (int line) {
    std::cout << "Exit at line " << line << "\n";
    exit(-1);
}

#define EXIT QuitMsg(__LINE__)

namespace google {
namespace protobuf {
namespace internal {

template <typename T>
T& RefAt(void* msg, ptrdiff_t offset) {
    return *reinterpret_cast<T*>(static_cast<char*>(msg) + offset);
}

inline uint64_t L64(const char* ptr) {
    uint64_t x;
    std::memcpy(&x, ptr, 8);
    return x;
}

__attribute__((noinline))
const char* Parse(const char* ptr, const char* end) {
    while (ptr < end) {
        uint32_t tag;
        ptr = ReadTag(ptr, &tag);
        if ((tag & 7) == 4) break;
        switch (tag >> 3) {
            case 1: {
                if (tag != 8) return nullptr;
                uint64_t x;
                ptr = VarintParse(ptr, &x);
                break;
            }
            case 2: {
                if (tag != 16 + 2) return nullptr;
                uint32_t sz = ReadSize(&ptr);
                ptr += sz;
                break;
            }
            case 3: {
                if (tag != 24 + 1) return nullptr;
                ptr += 8;
                break;
            }
            case 4: {
                if (tag != 32 + 5) return nullptr;
                ptr += 4;
                break;
            }
            case 5: {
                if (tag != 40 + 2) return nullptr;
                uint32_t sz = ReadSize(&ptr);
                ptr = Parse(ptr, ptr + sz);
                break;
            }
            case 6: {
                if (tag != 48 + 3) return nullptr;
                ptr = Parse(ptr, end);
                break;
            }
            default:
                return nullptr;
        }
    }
    return ptr;
}

template <bool use_preload>
__attribute__((noinline))
const char* ParseDirect(const char* ptr, const char* end) {
    uint32_t nexttag = *ptr;
    while (ptr < end) {
        uint32_t tag;
        uint64_t preload = L64(ptr + 2);
        ptr = ReadTag(ptr, &tag);
        uint32_t wt = use_preload ? nexttag & 7 : tag & 7;
        if (ABSL_PREDICT_FALSE(wt == 4)) break;
        asm volatile("");
        if (ABSL_PREDICT_FALSE(wt == 0)) {
            uint64_t x;
            ptr = VarintParse(ptr, &x);
            nexttag = preload;
            continue;
        }
        asm volatile("");
        if (ABSL_PREDICT_FALSE(wt == 1)) {
            ptr += 8;
            nexttag = preload >> (7 * 8);
            continue;
        }
        asm volatile("");
        if (ABSL_PREDICT_FALSE(wt == 5)) {
            ptr += 4;
            nexttag = preload >> (3 * 8);
            continue;
        }
        asm volatile("");
        if (ABSL_PREDICT_FALSE(wt == 3)) {
            ptr = Parse(ptr, end);
            nexttag = *ptr;
            continue;
        }
        asm volatile("");
        if (ABSL_PREDICT_TRUE(wt == 2)) {
            uint32_t sz = ReadSize(&ptr);
            if (tag == 40 + 2) {
                ptr = Parse(ptr, ptr + sz);                
            } else {
                ptr += sz;
            }
            nexttag = *ptr;
        } else {
            return nullptr;
        }
    }
    return ptr;
}

inline const TcParseTableBase::FieldEntry* FindFieldEntryInline(
    const TcParseTableBase* table, uint32_t field_num) {
  using FieldEntry = TcParseTableBase::FieldEntry;
    struct SkipEntry16 {
    uint16_t skipmap;
    uint16_t field_entry_offset;
    };

  const FieldEntry* const field_entries = table->field_entries_begin();

  uint32_t fstart = 1;
  uint32_t adj_fnum = field_num - fstart;

  if (ABSL_PREDICT_TRUE(adj_fnum < 32)) {
/*    uint32_t skipmap = table->skipmap32;
    uint32_t skipbit = 1 << adj_fnum;
    if (ABSL_PREDICT_FALSE(skipmap & skipbit)) return nullptr;
    skipmap &= skipbit - 1;
    adj_fnum -= absl::popcount(skipmap); */
    auto* entry = field_entries + adj_fnum;
    ABSL_ASSUME(entry != nullptr);
    return entry;
  }
  const uint16_t* lookup_table = table->field_lookup_begin();
  for (;;) {
#ifdef PROTOBUF_LITTLE_ENDIAN
    memcpy(&fstart, lookup_table, sizeof(fstart));
#else
    fstart = lookup_table[0] | (lookup_table[1] << 16);
#endif
    lookup_table += sizeof(fstart) / sizeof(*lookup_table);
    uint32_t num_skip_entries = *lookup_table++;
    if (field_num < fstart) return nullptr;
    adj_fnum = field_num - fstart;
    uint32_t skip_num = adj_fnum / 16;
    if (ABSL_PREDICT_TRUE(skip_num < num_skip_entries)) {
      // for each group of 16 fields we have:
      // a bitmap of 16 bits
      // a 16-bit field-entry offset for the first of them.
      auto* skip_data = lookup_table + (adj_fnum / 16) * (sizeof(SkipEntry16) /
                                                          sizeof(uint16_t));
      SkipEntry16 se = {skip_data[0], skip_data[1]};
      adj_fnum &= 15;
      uint32_t skipmap = se.skipmap;
      uint16_t skipbit = 1 << adj_fnum;
      if (ABSL_PREDICT_FALSE(skipmap & skipbit)) return nullptr;
      skipmap &= skipbit - 1;
      adj_fnum += se.field_entry_offset;
      adj_fnum -= absl::popcount(skipmap);
      auto* entry = field_entries + adj_fnum;
      ABSL_ASSUME(entry != nullptr);
      return entry;
    }
    lookup_table +=
        num_skip_entries * (sizeof(SkipEntry16) / sizeof(*lookup_table));
  }
}

inline void SetHasBit(void* x, TcParseTableBase::FieldEntry entry, void* dummy) {
    using namespace field_layout;
    x = (entry.type_card & kFcMask) == kFcSingular ? dummy : x;
    auto idx = entry.has_idx;
#if defined(__x86_64__) && defined(__GNUC__)
    asm("bts %1, %0\n" : "+m"(*static_cast<char*>(x)) : "r"(idx));
#else
    static_cast<char*>(x)[idx / 8] |= 1 << (idx & 7);
#endif
}

inline uint32_t GetSplitOffset(const TcParseTableBase* table) {
  return table->field_aux(kSplitOffsetAuxIdx)->offset;
}

inline uint32_t GetSizeofSplit(const TcParseTableBase* table) {
  return table->field_aux(kSplitSizeAuxIdx)->offset;
}

inline void* MaybeGetSplitBaseInline(MessageLite* msg, const bool is_split,
                                  const TcParseTableBase* table) {
  void* out = msg;
  if (ABSL_PREDICT_FALSE(is_split)) {
    const uint32_t split_offset = GetSplitOffset(table);
    void* default_split =
        TcParser::RefAt<void*>(table->default_instance, split_offset);
    void*& split = TcParser::RefAt<void*>(msg, split_offset);
    if (split == default_split) {
      // Allocate split instance when needed.
      uint32_t size = GetSizeofSplit(table);
      Arena* arena = msg->GetArena();
      split = (arena == nullptr) ? ::operator new(size)
                                 : arena->AllocateAligned(size);
      memcpy(split, default_split, size);
    }
    out = split;
  }
  return out;
}

inline void Store(uint64_t value, TcParseTableBase::FieldEntry entry, void* out, void* dummy) {
    using namespace field_layout;
    *static_cast<bool*>(out) = value;
    *static_cast<uint32_t*>((entry.type_card & kRepMask) == kRep32Bits ? out : dummy) = value;
    *static_cast<uint64_t*>((entry.type_card & kRepMask) == kRep64Bits ? out : dummy) = value;
}

template <typename T>
void AddRepeated(void* base, TcParseTableBase::FieldEntry entry, uint64_t value) {
    auto field = RefAt<RepeatedField<T>>(base, entry.offset);
    field.Add(value);
    // TODO add fast parsing the rest of repeated
}

inline uint32_t ParseScalarBranchless(uint32_t wt, uint64_t& data) {
    static const uint64_t size_mask[] = {
        0xFF,
        0xFFFF,
        0xFFFFFF,
        0xFFFFFFFF,
        0xFFFFFFFFFF,
        0xFFFFFFFFFFFF,
        0xFFFFFFFFFFFFFF,
        0xFFFFFFFFFFFFFFFF
    };
    uint64_t mask = 0x7f7f7f7f7f7f7f7f;
    auto x = data | mask;
    auto y = x ^ (x + 1);
    auto varintsize = __builtin_popcountll(y) / 8;
    auto fixedsize = wt & 4 ? 4 : 8;
    if (wt & 1) mask = -1;
    auto size = (wt & 1 ? fixedsize : varintsize);
    data &= size_mask[size - 1];
#ifdef __X86_64__
    data = _pext_u64(data, mask);
#else
    // TODO find good sequence for arm
#endif
    return size;
}

const char* TcParser::MiniParseLoop(MessageLite* msg, const char* ptr, ParseContext* ctx, 
        const TcParseTableBase* table, int64_t delta_or_group) {
    using namespace field_layout;

    // TODO move into ParseContext
    char dummy[8] = {};
    char has_dummy[8] = {};
    Arena* arena = msg->GetArena();
    while (!ctx->Done(&ptr)) {
        uint32_t tag;
        uint32_t wt = *ptr & 7;
        ptr = ReadTagInlined(ptr, &tag);
        if (ABSL_PREDICT_FALSE(wt == 4)) {
            if (delta_or_group != ~static_cast<int64_t>(tag - 1)) {
unusual_end:
                if (delta_or_group == -1) {
                    ctx->SetLastTag(tag);
                    return ptr;
                }
                return nullptr;
            }
            return ptr;
        }
        
        auto entry = *FindFieldEntryInline(table, tag >> 3);
        auto base = MaybeGetSplitBaseInline(msg, entry.type_card & kSplitMask, table);
        uint64_t value = L64(ptr);
        if (wt == 2) {
            switch (__builtin_expect(entry.type_card & kFkMask, kFkString)) {
                case kFkString:
                    break;
                case kFkMessage: {
                    auto sz = ReadSize(&ptr);
                    value = ctx->PushLimit(ptr, sz).token();
                    goto parse_submessage;
                }
                case kFkPackedFixed: {
                    auto sz = ReadSize(&ptr);
                    if ((entry.type_card & kRepMask) == kRep32Bits) {
                        ptr = ctx->ReadPackedFixed<uint32_t>(ptr, sz, &RefAt<RepeatedField<uint32_t>>(base, entry.offset));
                    } else {
                        ptr = ctx->ReadPackedFixed<uint64_t>(ptr, sz, &RefAt<RepeatedField<uint64_t>>(base, entry.offset));
                    }
                    continue;
                }
                case kFkPackedVarint: {
                    void* p = &RefAt<char>(base, entry.offset);
                    // TODO switch
                    ptr = ctx->ReadPackedVarint(ptr, [p, entry](uint64_t value) {
                        if (entry.type_card & kTvZigZag) value = WireFormatLite::ZigZagDecode64(value);
                        if ((entry.type_card & kRepMask) == kRep8Bits) {
                            static_cast<RepeatedField<bool>*>(p)->Add(value);
                        } else if ((entry.type_card & kRepMask) == kRep32Bits) {
                            static_cast<RepeatedField<uint32_t>*>(p)->Add(value);
                        } else {
                            static_cast<RepeatedField<uint64_t>*>(p)->Add(value);
                        }
                    });
                    continue;
                }
                case kFkMap: {
                    TcFieldData data(tag | (static_cast<uint64_t>(entry.offset) << 32));
                    if (entry.type_card & kSplitMask) {
                        ptr = TcParser::MpMap<true>(msg, ptr, ctx, data, table, 0);
                    } else {
                        ptr = TcParser::MpMap<false>(msg, ptr, ctx, data, table, 0);
                    }
                    continue;
                }
                default:
                    goto unusual;
            }
            switch (__builtin_expect(entry.type_card & kRepMask, kRepAString)) {
                case kRepAString:
                    break;
                default:
                    // TODO
                    EXIT;
            }
            if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) <= kFcOptional)) {
                SetHasBit(base, entry, dummy);
            } else if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) == kFcRepeated)) {
                auto& field = RefAt<RepeatedPtrField<std::string>>(base, entry.offset);
                auto sz = ReadSize(&ptr);
                ptr = ctx->ReadString(ptr, sz, field.Add());
                if (ptr == nullptr) return nullptr;
                continue;
            } else {
                if (ChangeOneof(table, entry, tag >> 3, ctx, msg)) {
                    RefAt<ArenaStringPtr>(msg, entry.offset).InitDefault();
                }
            }
            ptr = ctx->ReadArenaString(ptr, &RefAt<ArenaStringPtr>(msg, entry.offset), arena);
            if (ptr == nullptr) return nullptr;
            continue;
        } else {
            if (ABSL_PREDICT_FALSE(wt == 3)) {
                if ((entry.type_card & kFkMask) != kFkMessage) goto unusual;
                value = ~static_cast<uint64_t>(tag);
                goto parse_submessage;
            }
            if (ABSL_PREDICT_FALSE(wt > 5)) return nullptr;
            ptr += ParseScalarBranchless(wt, value);
            if ((entry.type_card & kTvMask) == kTvZigZag) value = WireFormatLite::ZigZagDecode64(value);
            if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) <= kFcOptional)) {
                SetHasBit(base, entry, has_dummy);
            } else if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) == kFcRepeated)) {
                if ((entry.type_card & kRepMask) == kRep8Bits) {
                    AddRepeated<bool>(base, entry, value);
                } else if ((entry.type_card & kRepMask) == kRep32Bits) {
                    AddRepeated<uint32_t>(base, entry, value);
                } else {
                    AddRepeated<uint64_t>(base, entry, value);
                }
                continue;
            } else {
                ChangeOneof(table, entry, tag >> 3, ctx, msg);
            }
            Store(value, entry, &RefAt<char>(base, entry.offset), dummy);
            continue;
        }
parse_submessage:
        {
            auto aux_idx = entry.aux_idx; 
            auto child_table = table->field_aux(aux_idx)->table;
            MessageLite* child;
            if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) <= kFcOptional)) {
                SetHasBit(base, entry, has_dummy);
                auto& field = RefAt<MessageLite*>(base, entry.offset);
                if (field == nullptr) {
                    field = table->default_instance->New(arena);
                }
                child = field;
            } else if (ABSL_PREDICT_TRUE((entry.type_card & kFcMask) == kFcRepeated)) {
                auto& field = RefAt<RepeatedPtrFieldBase>(base, entry.offset);
                child = field.template Add<GenericTypeHandler<MessageLite>>(table->default_instance);
            } else {
                auto& field = RefAt<MessageLite*>(base, entry.offset);
                if (ChangeOneof(table, entry, tag >> 3, ctx, msg)) {
                    field = table->default_instance->New(arena);
                }
                child = field;
            }
            ptr = MiniParseLoop(child, ptr, ctx, child_table, value);
            if (ptr == nullptr) return nullptr;
            continue;
        }
unusual:
        if (tag == 0) goto unusual_end;
        // TODO packed/unpacked repeated fields mismatch
        ptr = table->fallback(msg, ptr, ctx, TcFieldData(tag), table, 0);
    }
    if (ptr == nullptr) return nullptr;
    if (delta_or_group >= 0) {
        (void)ctx->PopLimit(EpsCopyInputStream::LimitToken(delta_or_group));
        return ptr;
    } else if (delta_or_group == -1) {
        return ptr;
    } else {
        return nullptr;
    } 
}


__attribute__((noinline))
const char* ParseTest(const char* ptr, const char* end) {
    uint32_t nexttag = *ptr;
    while (ptr < end) {
        uint32_t tag = nexttag;
        uint64_t data = L64(ptr);
        auto x = data | 0x7f7f7f7f7f7f7f7f;
        auto y = x ^ (x + 1);
        auto tagsize = __builtin_popcountll(y) / 8;
        auto fixedsize = tagsize + (tag & 4 ? 4 : 8); 
        x |= y;
        y = x ^ (x + 1);
        auto varintsz = __builtin_popcountll(y) / 8;
        auto nextptr = ptr + (tag & 1 ? fixedsize : varintsz);
        nexttag = *nextptr;
        asm volatile(""::"r"(nexttag));
        if (__builtin_expect((tag & 7) == 2, 0)) {
                ptr++;
                uint32_t sz = ReadSize(&ptr);
                ptr += sz;
                nexttag = *ptr;
        } else {
            ptr = nextptr;
        }
        /*switch (tag & 7) {
            case 0:
            case 1:
            case 5:
                ptr = nextptr;
                break;
            case 2: {
                ptr++;
                uint32_t sz = ReadSize(&ptr);
                ptr += sz;
                nexttag = *ptr;
                break;
            }
            case 3:
                asm volatile("");
            case 4:
                asm volatile("");
            case 6:
                asm volatile("");
                return nullptr;
            case 7:
                asm volatile("");
                return nullptr;
        }*/
    }
    return ptr;
}

enum {
    // singular = 0, repeated = 1, oneof = 2
    kCardinality = 0x300,
    kExcessHasbits = 0x2000,
    // Primitives
    k64bit = 0x400,
    kZigZag = 0x800,
    kBool = 0x1000,
    // Length delimited
    // string = 0, message = 1, map = 2, cord, stringview, packed bool, uint32, sint32, uint64, sint64, f32, f64
    kMessage = 0x400,
    kCheckUtf8 = 0x800,
};

struct AuxData {
    uint32_t offset;
    uint32_t oneof;
    const void* table;
};

struct ParseTable {
    const void* default_instance;
    const AuxData* aux;
    int offset_hasbits;
    int start_field_num;
    int entry_cutoff;
    const uint64_t* entries;
};

const uint64_t test_proto_sub_group_entries[] = {
    0x0018000000010008 | kZigZag,  // optional sint32 varint = 1;
};

const ParseTable test_proto_sub_group_parse_table = {
    &test_benchmark::_TestProto_SubGroup_default_instance_,
    nullptr,
    16,
    1,
    1,
    test_proto_sub_group_entries,
};

const uint64_t test_proto_entries[] = {
    0x0030000000080008,  // optional int32 varint = 1;
    0x0018000000010012,  // optional bytes bytes = 2;
    0x0038000000200019 | k64bit,  // optional fixed64 fixed64 = 3;
    0x0034000000100025,  // optional fixed32 fixed32 = 4;
    0x000000000002002A | kMessage,  // optional TestProto recurse = 5;
    0x0001000000040033,  // optional group sub_group = 6;
};

extern const ParseTable test_proto_parse_table;
const AuxData test_proto_aux[] = {
    {0x20, 0, &test_proto_parse_table},
    {0x28, 0, &test_proto_sub_group_parse_table},
};

const ParseTable test_proto_parse_table = {
    &test_benchmark::_TestProto_default_instance_,
    test_proto_aux,
    16,
    1,
    6,
    test_proto_entries
};

class Utf8Checker {
public:
    bool BatchTest(absl::string_view s, bool do_test) {
        utf8_tests[utf8_idx] = s;
        utf8_idx += int(do_test);
        if (ABSL_PREDICT_FALSE(utf8_idx == kNumUtf8Tests)) {
            return DoTests();
        }
        return true;
    }

    bool DoTests() {
        for (unsigned i = 0; i < utf8_idx; i++) if (!VerifyUTF8(utf8_tests[i], "")) return false;
        utf8_idx = 0;
        return true;
    }

private:
    static constexpr unsigned kNumUtf8Tests = 10;
    absl::string_view utf8_tests[kNumUtf8Tests];
    unsigned utf8_idx = 0;
};

Utf8Checker utf8_checker;

__attribute__((noinline))
const char* ParseProto(MessageLite* msg, const char* ptr, ParseContext* ctx, const ParseTable* table, int delta_or_group_num) {
    //if (--ctx->depth_ == 0) return nullptr;

    #define ERROR do { if (1) std::cout << __LINE__ << " failure\n"; goto error; } while (0)

    uint64_t hasbits = 0;
    if (table->offset_hasbits) hasbits = uint64_t(RefAt<uint32_t>(msg, table->offset_hasbits)) << 16;
    while (!ctx->Done(&ptr)) {
        uint32_t tag = uint8_t(*ptr);
        uint32_t field_num;
        ptr = ReadTagInlined(ptr, &field_num);
        if (ptr == nullptr) ERROR;
        field_num >>= 3;
        if (ABSL_PREDICT_FALSE((tag & 7) == 4)) {
            if (field_num != -delta_or_group_num) ERROR;
            goto group_end;
        }
        uint64_t entry;
        if (ABSL_PREDICT_FALSE(field_num - table->start_field_num >= table->entry_cutoff)) {
            if (tag == 0) {
                goto group_end;
            }
            // TODO search in sparse list
            goto unknown_field;
        } else {
            entry = table->entries[field_num - table->start_field_num];
        }
        asm("":: "r"(entry));
        if (uint8_t(entry) != uint8_t(tag)) {
unknown_field:
            // TODO unknown field
            ERROR;
        }
        int delta;
        uint64_t data = L64(ptr);
       // asm volatile(""::"r"(data));
        if (ABSL_PREDICT_FALSE((tag & 7) == 2)) {
            uint32_t sz = data & 0xFF; ptr++;
            //uint32_t sz = ReadSize(&ptr);
            if (ptr == nullptr) ERROR;
            if (ABSL_PREDICT_TRUE((entry & kMessage) == 0)) {
                auto offset = entry >> 48;
                RefAt<ArenaStringPtr>(msg, offset).SetBytes(ptr, sz, nullptr);
                utf8_checker.BatchTest(RefAt<ArenaStringPtr>(msg, offset).Get(), entry & kCheckUtf8);
                ptr += sz;
            } else {
                delta = ctx->PushLimit(ptr, sz).token();
                goto parse_submsg;
            }
            if (ABSL_PREDICT_FALSE(entry & (kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
            } else {
                hasbits |= entry;
            }
        } else {
            static const uint64_t size_mask[] = {
                0xFF,
                0xFFFF,
                0xFFFFFF,
                0xFFFFFFFF,
                0xFFFFFFFFFF,
                0xFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFFFF
            };
            if (ABSL_PREDICT_FALSE((tag & 7) == 3)) {
                delta = -field_num;
parse_submsg:
                auto aux_idx = entry >> 48;
                unsigned offset = table->aux[aux_idx].offset;
                const ParseTable* child_table = static_cast<const ParseTable*>(table->aux[aux_idx].table);
                MessageLite*& child = RefAt<MessageLite*>(msg, offset);
                if (child == nullptr) {
                    child = static_cast<const MessageLite*>(child_table->default_instance)->New();
                }
                ptr = ParseProto(child, ptr, ctx, child_table, delta);
                if (ptr == nullptr) ERROR;
                continue;
            }
            uint64_t mask = 0x7f7f7f7f7f7f7f7f;
            auto x = data | mask;
            auto y = x ^ (x + 1);
            auto varintsize = __builtin_popcountll(y) / 8;
            auto fixedsize = tag & 4 ? 4 : 8;
            if (tag & 1) mask = -1;
            auto size = (tag & 1 ? fixedsize : varintsize);
            data &= size_mask[size - 1];
#ifdef __X86_64__
            data = _pext_u64(data, mask);
#else
            // TODO find good sequence for arm
#endif
            if (entry & kZigZag) data = WireFormatLite::ZigZagDecode64(data);
            if (ABSL_PREDICT_FALSE(entry & (kBool | kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
            } else {
                hasbits |= entry;
            }
            auto offset = entry >> 48;
            RefAt<uint32_t>(msg, offset + ((entry & k64bit) ? 4 : 0)) = data >> 32;
            RefAt<uint32_t>(msg, offset) = data;
            ptr += size;
        }
    }
    if (delta_or_group_num < 0) ERROR;
    if (delta_or_group_num > 0) {
        if (!ctx->PopLimit(EpsCopyInputStream::LimitToken(delta_or_group_num))) ERROR;
    }
group_end:
    // Sync hasbits
    if (table->offset_hasbits) RefAt<uint32_t>(msg, table->offset_hasbits) = hasbits >> 16;
    // ctx->depth_++;
    return ptr;
error:
    ptr = nullptr;
    goto group_end;
    #undef ERROR
}

#if 0
__attribute__((noinline))
const char* ParseProto2(MessageLite* msg, const char* ptr, ParseContext* ctx, const ParseTable* table, int delta_or_group_num) {
    //if (--ctx->depth_ == 0) return nullptr;

    #define ERROR do { if (0) std::cout << __LINE__ << " failure\n"; goto error; } while (0)

    uint64_t hasbits = 0;
    if (table->offset_hasbits) hasbits = uint64_t(RefAt<uint32_t>(msg, table->offset_hasbits)) << 16;
    while (!ctx->Done(&ptr)) {
        uint32_t opcode = uint8_t(*ptr) & 7;
        uint32_t tag;
        ptr = ReadTagInlined(ptr, &tag);
        if (ptr == nullptr) ERROR;
        if (ABSL_PREDICT_FALSE((tag & 7) == 4)) {
            if (tag != -delta_or_group_num) ERROR;
            goto group_end;
        }
        uint32_t idx = (tag >> 3) - table->start_field_num;
        uint64_t entry;
        if (ABSL_PREDICT_FALSE(idx >= table->entry_cutoff)) {
            if (tag == 0) {
                goto group_end;
            }
            // TODO search in sparse list
            goto unknown_field;
        } else {
            entry = table->entries[idx];
        }
        asm("":: "r"(entry));
        if (uint8_t(entry & 7) != uint8_t(opcode)) {
unknown_field:
            // TODO unknown field
            ERROR;
        }
        // Here we are guaranteed that opcode is a valid wiretype (ie. 0, ..., 5) assuming correctness of the tables.
        // Here we are at the crux of proto parsing performance. What to do next depends on the field indicated by the
        // tag. In production where in between proto parsing 100 millions of other instructions are run, poluting the
        // branch predictor caches to make it unlikely to achieve good hit rates for protos with sufficient mix of types
        // fields.
        // The flexibility of protos results in a huge set of types. The bigger the sets the less chance of a correct
        // target predict. When a branch miss happens this can be modelled as a explicit dependency of all subsequent
        // instructions on that branch instruction, including the latency of the resteer (pipeline bubble). In case
        // of a correct prediction the latency of the computation of the branch target do not matter for the
        // throughput of the body of the parse loop. The fundamental dependency chain is carried by ptr.
        //
        // The original parser, which was generated code contained a switch on field number. Which leads to
        // 1) ptr -> tag = Load(ptr) -> target = Load(jump_table + tag) -> branch miss -> ptr += size(field)
        // Which contains two load-to-use latency + branch miss latency on the critical path (5 + 5 + 15 = 25 cycles).
        //
        // In a naive table driven parser, you'd store a fieldtype in a table and switch on the fieldtype in a very
        // big universal parse loop. Unfortunately this adds an extra load-to-use on the critical path. Notice you
        // don't see this effect in micro benchmarks where unless you are careful the BP is able to memorize the data
        // perfectly,
        // 2) ptr -> tag = Load(ptr) -> type -> parse_t[tag] -> target = jump_t[type] -> branch miss -> ptr += size(field)
        //
        // The crux of the TCParser is that using tail calling we can embed the function pointers directly in the
        // parse tables and hence approaching the performance of the generated code.
        //
        // To fundamentally improve the performance we need to structurally improve the above bottlenecks. Improving
        // hit rates / reducing the throughput latency. For instance conditional branches have a fixed target, so 
        // they do not involve an additional load from a jump table, potentially shaving off cycles. However there
        // is a competing concern where it's very expensive to have further additional branch misses in a single 
        // parse loop iteration. If one incurs a branch miss, it's imperative to learn as much as possible from the
        // branch miss to minimize any additional branch miss.
        //
        // There is no best solution for all cases, everything decission is a trade off. There is only good solutions
        // optimizing a workload representative of production. Now there are certain pertinent observations.
        // Clearly not all field types are equally common. For instance it's clear from fleetbench protos, that claim
        // to be a representable to at least googles workload, that singular strings/bytes and primitive scalars
        // constitutes the far majority of fields. 
        //
        // The idea is too separate fields into common / easy and rare / complex. We can take care of the rare/complex
        // by the chain 2. Yes it has an extra load, but the fast majority it will branch to the common case, which
        // will be accurately predicted and hence the chain does not participate in the bottleneck. Importantly,
        // when a mispredict does happen we jump to a fine grained parser dedicated to dealing with the field.
        //
        // The common / easy case should happen > 80(90) percent of the time. In this case we should find a code
        // path with a substantial lower critical chain than the TCParser. Here it pays to try as much as possible
        // using branchless code to unify different field paths. Instead of branching on the field number
        // we can branch on wiretype, determining the structure of the field to follow. Because the wiretypes are
        // very limited (only 6) we will use conditional branches removing an additional load fron the chain on
        // branch miss. We use branchless code to unify singular int32/int64 zigzag/normal into one branch for varints.
        // Fixed 32 and 64 wiretypes are automatically generic for all different types. For length delimited
        // bytes and strings we branchless batch utf8 verification. 

        switch ((entry >> 3) & 15) {
            case 0:
                goto common_case;
            case 1:
                // singular bool
            case 2:
                // repeated bool
            case 3:
                // repeated int32/uint32
            case 4:
                // repeated sint32
            case 5:
                // repeated int64/uint64
            case 6:
                // repeated sint64
            case 7:
                // repeated fixed32
            case 8:
                // repeated fixed64            
            case 9:
                // repeated bytes + debug string
            case 10:
                // repeated string
            case 11:
                // repeated message
            case 12:
                // repeated group
            case 13:
                // packed bool
            case 14:
                // packed int32/uint32
            case 15:
                // packed sint32
            case 16:
                // packed int64/uint64
            case 17:
                // packed sint64
            case 18:
                // packed fixed32
            case 19:
                // packed fixed64
            case 7:
                // oneof bool
            case 8:
                // oneof uint32/int32/sint32/uint64/int64/sint64
            case 8:
                // oneof fixed32/fixed64
            case 8:
                // oneof bytes/string
            case 7:
                // oneof message
            case 7:
                // oneof group
            
            default:
                goto error;
        }
        continue;
common_case:
        int delta;
        if (ABSL_PREDICT_FALSE(opcode == 2)) {
            uint32_t sz = ReadSize(&ptr);
            if (ptr == nullptr) ERROR;
            if (ABSL_PREDICT_TRUE((entry & kMessage) == 0)) {
                auto offset = entry >> 48;
                RefAt<ArenaStringPtr>(msg, offset).SetBytes(ptr, sz, nullptr);
                utf8_checker.BatchTest(RefAt<ArenaStringPtr>(msg, offset).Get(), entry & kCheckUtf8);
                ptr += sz;
            } else {
                delta = ctx->PushLimit(ptr, sz).token();
                goto parse_submsg;
            }
            if (ABSL_PREDICT_FALSE(entry & (kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
            } else {
                hasbits |= entry;
            }
        } else {
            static const uint64_t size_mask[] = {
                0xFF,
                0xFFFF,
                0xFFFFFF,
                0xFFFFFFFF,
                0xFFFFFFFFFF,
                0xFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFFFF
            };
            if (ABSL_PREDICT_FALSE(opcode == 3)) {
                //delta = -field_num;
parse_submsg:
                auto aux_idx = entry >> 48;
                unsigned offset = table->aux[aux_idx].offset;
                const ParseTable* child_table = static_cast<const ParseTable*>(table->aux[aux_idx].table);
                MessageLite*& child = RefAt<MessageLite*>(msg, offset);
                if (child == nullptr) {
                    child = static_cast<const MessageLite*>(child_table->default_instance)->New();
                }
                ptr = ParseProto(child, ptr, ctx, child_table, delta);
                if (ptr == nullptr) ERROR;
                continue;
            }
            uint64_t data = L64(ptr);
            uint64_t mask = 0x7f7f7f7f7f7f7f7f;
            auto x = data | mask;
            auto y = x ^ (x + 1);
            auto varintsize = __builtin_popcountll(y) / 8;
            auto fixedsize = tag & 4 ? 4 : 8;
            if (tag & 1) mask = -1;
            auto size = (tag & 1 ? fixedsize : varintsize);
            data &= size_mask[size - 1];
#ifdef __X86_64__
            data = _pext_u64(data, mask);
#else
            // TODO find good sequence for arm
#endif
            if (entry & kZigZag) data = WireFormatLite::ZigZagDecode64(data);
            if (ABSL_PREDICT_FALSE(entry & (kBool | kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
            } else {
                hasbits |= entry;
            }
            auto offset = entry >> 48;
            RefAt<uint32_t>(msg, offset + ((entry & k64bit) ? 4 : 0)) = data >> 32;
            RefAt<uint32_t>(msg, offset) = data;
            ptr += size;
        }
    }
    if (delta_or_group_num < 0) ERROR;
    if (delta_or_group_num > 0) {
        if (!ctx->PopLimit(EpsCopyInputStream::LimitToken(delta_or_group_num))) ERROR;
    }
group_end:
    // Sync hasbits
    if (table->offset_hasbits) RefAt<uint32_t>(msg, table->offset_hasbits) = hasbits >> 16;
    // ctx->depth_++;
    return ptr;
error:
    ptr = nullptr;
    goto group_end;
}
#endif


void WriteRandom(std::string* s, int iters, int level) {
    io::StringOutputStream os(s);
    io::CodedOutputStream out(&os);
    std::mt19937 gen(0x3523fa4f);
    std::uniform_int_distribution pick(1, 6);
    for (int i = 0; i < iters; i++) {
again:
        auto tag = pick(gen);
        switch (tag) {
            case 1:
                out.WriteTag(tag * 8 + 0);
                out.WriteVarint32(20);
                break;
            case 2:
                if (level < 1) goto again;
                out.WriteTag(tag * 8 + 2);
                out.WriteVarint32(5);
                out.WriteString("Hello");
                break;
            case 3:
                out.WriteTag(tag * 8 + 1);
                out.WriteLittleEndian64(0xdeadbeef);
                break;
            case 4:
                out.WriteTag(tag * 8 + 5);
                out.WriteLittleEndian32(0xdead);
                break;
            case 5:
                if (level < 2) goto again;
                out.WriteTag(tag * 8 + 2);
                out.WriteVarint32(2);
                out.WriteTag(8 + 0);
                out.WriteVarint32(1);
                break;
            case 6:
                if (level < 2) goto again;
                out.WriteTag(tag * 8 + 3);
                out.WriteTag(8 + 0);
                out.WriteVarint32(WireFormatLite::ZigZagEncode32(-30));
                out.WriteTag(tag * 8 + 4);
                break;
            default:
                EXIT;
        }
    }
}
#if 0
static void BM_RegularParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, state.range(0), 2);
   for (auto _ : state) {
        if (Parse(x.data(), x.data() + x.size()) == nullptr) EXIT;
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RegularParse)->Range(1024, 256 * 1024)->RangeMultiplier(4);

template <bool use_preload>
static void BM_ParseDirect(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, state.range(0), 2);
    for (auto _ : state) {
        if (ParseDirect<use_preload>(x.data(), x.data() + x.size()) == nullptr) EXIT;
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK_TEMPLATE(BM_ParseDirect, false)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_TEMPLATE(BM_ParseDirect, true)->Range(1024, 256 * 1024)->RangeMultiplier(4);
#endif
// This pattern allows one to legally access private members, which we need to
// implement ArenaString. By using legal c++ we ensure that we do not break
// strict aliasing preventing potential miscompiles.
template <typename Tag, typename Tag::type M>
struct Robber {
    friend constexpr typename Tag::type Get(Tag) {
        return M;
    }
};

struct GetTable {
    using type = const TcParseTable<
      3, 6, 2,
      0, 2>*;
    friend constexpr type Get(GetTable);
};

template
struct Robber<GetTable, &test_benchmark::TestProto::_table_>;


static void BM_MiniParseLoop(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    x.reserve(x.size() + 10000000);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        const char* ptr;
        ParseContext ctx(20, false, &ptr, x);
        if (TcParser::MiniParseLoop(&proto, ptr, &ctx, &Get(GetTable())->header, 0) == nullptr) EXIT;
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK_CAPTURE(BM_MiniParseLoop, nostring, 0)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_MiniParseLoop, string, 1)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_MiniParseLoop, submsg, 2)->Range(1024, 256 * 1024)->RangeMultiplier(4);

#if 0
static void BM_NewParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, state.range(0), 0);
    for (auto _ : state) {
        if (ParseTest(x.data(), x.data() + x.size()) == nullptr) EXIT;
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_NewParse)->Range(1024, 256 * 1024)->RangeMultiplier(4);
#endif

static void BM_Proto2Parse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        proto.ParseFromString(x);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK_CAPTURE(BM_Proto2Parse, nostring, 0)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_Proto2Parse, string, 1)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_Proto2Parse, submsg, 2)->Range(1024, 256 * 1024)->RangeMultiplier(4);

#if 0

int64_t buf[8191];

static void BM_UpbParse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        upb_Arena* arena = upb_Arena_Init(buf, sizeof(buf), nullptr);
        auto* proto =
            test_benchmark_TestProto_parse_ex(x.data(), x.size(), nullptr, 0, arena);
        if (!proto) {
            printf("Failed to parse.\n");
            exit(1);
        }
        upb_Arena_Free(arena);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_UpbParse, nostring, 0);
BENCHMARK_CAPTURE(BM_UpbParse, string, 1);
BENCHMARK_CAPTURE(BM_UpbParse, submsg, 2);

#endif

void TestParse() {
    test_benchmark::TestProto proto;
    proto.set_varint(10);
    proto.set_bytes("Cool");
    proto.set_fixed64(64);
    proto.set_fixed32(32);
    {
        auto child = proto.mutable_recurse();
        child->set_varint(1000);
        child->set_bytes("CoolCool");
        child->set_fixed64(640);
        child->set_fixed32(320);
    }
    {
        auto child = proto.mutable_subgroup();
        child->set_varint(-20);
    }
    auto serialized = proto.SerializeAsString();
    proto.Clear();
    const char* ptr;
    ParseContext ctx(20, false, &ptr, serialized);
    ptr = ParseProto(&proto, ptr, &ctx, &test_proto_parse_table, 0);
    if (ptr == nullptr) {
        std::cout << "Parse fail\n";
        EXIT;
    }
    std::string s;
    json::PrintOptions print_options{true};
    json::MessageToJsonString(proto, &s, print_options).IgnoreError();
    std::cout << s;
}

static void BM_TableParse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        const char* ptr;
        ParseContext ctx(20, false, &ptr, x);
        ParseProto(&proto, ptr, &ctx, &test_proto_parse_table, 0);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK_CAPTURE(BM_TableParse, nostring, 0)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_TableParse, string, 1)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_TableParse, submsg, 2)->Range(1024, 256 * 1024)->RangeMultiplier(4);


}  // namespace internal
}  // namespace protobuf
}  // namespace google
