// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/protobuf/cpp_features.proto
// Protobuf C++ Version: 4.26.0-dev

#ifndef GOOGLE_PROTOBUF_INCLUDED_google_2fprotobuf_2fcpp_5ffeatures_2eproto_2epb_2eh
#define GOOGLE_PROTOBUF_INCLUDED_google_2fprotobuf_2fcpp_5ffeatures_2eproto_2epb_2eh

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "google/protobuf/port_def.inc"
#if PROTOBUF_VERSION != 4026000
#error "Protobuf C++ gencode is built with an incompatible version of"
#error "Protobuf C++ headers/runtime. See"
#error "https://protobuf.dev/support/cross-version-runtime-guarantee/#cpp"
#endif
#include "google/protobuf/port_undef.inc"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/arenastring.h"
#include "google/protobuf/generated_message_tctable_decl.h"
#include "google/protobuf/generated_message_util.h"
#include "google/protobuf/metadata_lite.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/message.h"
#include "google/protobuf/repeated_field.h"  // IWYU pragma: export
#include "google/protobuf/extension_set.h"  // IWYU pragma: export
#include "google/protobuf/unknown_field_set.h"
#include "google/protobuf/descriptor.pb.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"

#define PROTOBUF_INTERNAL_EXPORT_google_2fprotobuf_2fcpp_5ffeatures_2eproto PROTOBUF_EXPORT

namespace google {
namespace protobuf {
namespace internal {
class AnyMetadata;
}  // namespace internal
}  // namespace protobuf
}  // namespace google

// Internal implementation detail -- do not use these members.
struct PROTOBUF_EXPORT TableStruct_google_2fprotobuf_2fcpp_5ffeatures_2eproto {
  static const ::uint32_t offsets[];
};
PROTOBUF_EXPORT extern const ::google::protobuf::internal::DescriptorTable
    descriptor_table_google_2fprotobuf_2fcpp_5ffeatures_2eproto;
namespace pb {
class CppFeatures;
struct CppFeaturesDefaultTypeInternal;
PROTOBUF_EXPORT extern CppFeaturesDefaultTypeInternal _CppFeatures_default_instance_;
}  // namespace pb
namespace google {
namespace protobuf {
}  // namespace protobuf
}  // namespace google

namespace pb {

// ===================================================================


// -------------------------------------------------------------------

class PROTOBUF_EXPORT CppFeatures final  : public ::google::protobuf::Message
/* @@protoc_insertion_point(class_definition:pb.CppFeatures) */ {
 public:
  inline CppFeatures() : CppFeatures(nullptr) {}
  ~CppFeatures() override;
  template <typename = void>
  explicit PROTOBUF_CONSTEXPR CppFeatures(
      ::google::protobuf::internal::ConstantInitialized);

  inline CppFeatures(const CppFeatures& from) : CppFeatures(nullptr, from) {}
  inline CppFeatures(CppFeatures&& from) noexcept
      : CppFeatures(nullptr, std::move(from)) {}
  inline CppFeatures& operator=(const CppFeatures& from) {
    CopyFrom(from);
    return *this;
  }
  inline CppFeatures& operator=(CppFeatures&& from) noexcept {
    if (this == &from) return *this;
    if (GetArena() == from.GetArena()
#ifdef PROTOBUF_FORCE_COPY_IN_MOVE
        && GetArena() != nullptr
#endif  // !PROTOBUF_FORCE_COPY_IN_MOVE
    ) {
      InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance);
  }
  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields()
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return _internal_metadata_.mutable_unknown_fields<::google::protobuf::UnknownFieldSet>();
  }

  static const ::google::protobuf::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::google::protobuf::Descriptor* GetDescriptor() {
    return default_instance().GetMetadata().descriptor;
  }
  static const ::google::protobuf::Reflection* GetReflection() {
    return default_instance().GetMetadata().reflection;
  }
  static const CppFeatures& default_instance() {
    return *internal_default_instance();
  }
  static inline const CppFeatures* internal_default_instance() {
    return reinterpret_cast<const CppFeatures*>(
        &_CppFeatures_default_instance_);
  }
  static constexpr int kIndexInFileMessages = 0;
  friend void swap(CppFeatures& a, CppFeatures& b) { a.Swap(&b); }
  inline void Swap(CppFeatures* other) {
    if (other == this) return;
#ifdef PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() != nullptr && GetArena() == other->GetArena()) {
#else   // PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetArena() == other->GetArena()) {
#endif  // !PROTOBUF_FORCE_COPY_IN_SWAP
      InternalSwap(other);
    } else {
      ::google::protobuf::internal::GenericSwap(this, other);
    }
  }
  void UnsafeArenaSwap(CppFeatures* other) {
    if (other == this) return;
    ABSL_DCHECK(GetArena() == other->GetArena());
    InternalSwap(other);
  }

  // implements Message ----------------------------------------------

  CppFeatures* New(::google::protobuf::Arena* arena = nullptr) const final {
    return ::google::protobuf::Message::DefaultConstruct<CppFeatures>(arena);
  }
  using ::google::protobuf::Message::CopyFrom;
  void CopyFrom(const CppFeatures& from);
  using ::google::protobuf::Message::MergeFrom;
  void MergeFrom(const CppFeatures& from) { CppFeatures::MergeImpl(*this, from); }

  private:
  static void MergeImpl(
      ::google::protobuf::MessageLite& to_msg,
      const ::google::protobuf::MessageLite& from_msg);

  public:
  ABSL_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  ::size_t ByteSizeLong() const final;
  const char* _InternalParse(const char* ptr, ::google::protobuf::internal::ParseContext* ctx) final;
  ::uint8_t* _InternalSerialize(
      ::uint8_t* target,
      ::google::protobuf::io::EpsCopyOutputStream* stream) const final;
  int GetCachedSize() const { return _impl_._cached_size_.Get(); }

  private:
  void SharedCtor(::google::protobuf::Arena* arena);
  void SharedDtor();
  void InternalSwap(CppFeatures* other);
 private:
  friend class ::google::protobuf::internal::AnyMetadata;
  static ::absl::string_view FullMessageName() { return "pb.CppFeatures"; }

 protected:
  explicit CppFeatures(::google::protobuf::Arena* arena);
  CppFeatures(::google::protobuf::Arena* arena, const CppFeatures& from);
  CppFeatures(::google::protobuf::Arena* arena, CppFeatures&& from) noexcept
      : CppFeatures(arena) {
    *this = ::std::move(from);
  }
  const ::google::protobuf::MessageLite::ClassData* GetClassData()
      const final;

 public:
  ::google::protobuf::Metadata GetMetadata() const final;
  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------
  enum : int {
    kLegacyClosedEnumFieldNumber = 1,
  };
  // optional bool legacy_closed_enum = 1 [retention = RETENTION_RUNTIME, targets = TARGET_TYPE_FIELD, targets = TARGET_TYPE_FILE, edition_defaults = {
  bool has_legacy_closed_enum() const;
  void clear_legacy_closed_enum() ;
  bool legacy_closed_enum() const;
  void set_legacy_closed_enum(bool value);

  private:
  bool _internal_legacy_closed_enum() const;
  void _internal_set_legacy_closed_enum(bool value);

  public:
  // @@protoc_insertion_point(class_scope:pb.CppFeatures)
 private:
  class _Internal;
  friend class ::google::protobuf::internal::TcParser;
  static const ::google::protobuf::internal::TcParseTable<
      1, 1, 0,
      0, 2>
      _table_;
  friend class ::google::protobuf::MessageLite;
  friend class ::google::protobuf::Arena;
  template <typename T>
  friend class ::google::protobuf::Arena::InternalHelper;
  using InternalArenaConstructable_ = void;
  using DestructorSkippable_ = void;
  struct Impl_ {
    inline explicit constexpr Impl_(
        ::google::protobuf::internal::ConstantInitialized) noexcept;
    inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                          ::google::protobuf::Arena* arena);
    inline explicit Impl_(::google::protobuf::internal::InternalVisibility visibility,
                          ::google::protobuf::Arena* arena, const Impl_& from);
    ::google::protobuf::internal::HasBits<1> _has_bits_;
    mutable ::google::protobuf::internal::CachedSize _cached_size_;
    bool legacy_closed_enum_;
    PROTOBUF_TSAN_DECLARE_MEMBER
  };
  union { Impl_ _impl_; };
  friend struct ::TableStruct_google_2fprotobuf_2fcpp_5ffeatures_2eproto;
};

// ===================================================================



static const int kCppFieldNumber = 1000;
PROTOBUF_EXPORT extern ::google::protobuf::internal::ExtensionIdentifier<
    ::google::protobuf::FeatureSet, ::google::protobuf::internal::MessageTypeTraits< ::pb::CppFeatures >, 11,
    false>
    cpp;

// ===================================================================


#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif  // __GNUC__
// -------------------------------------------------------------------

// CppFeatures

// optional bool legacy_closed_enum = 1 [retention = RETENTION_RUNTIME, targets = TARGET_TYPE_FIELD, targets = TARGET_TYPE_FILE, edition_defaults = {
inline bool CppFeatures::has_legacy_closed_enum() const {
  bool value = (_impl_._has_bits_[0] & 0x00000001u) != 0;
  return value;
}
inline void CppFeatures::clear_legacy_closed_enum() {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.legacy_closed_enum_ = false;
  _impl_._has_bits_[0] &= ~0x00000001u;
}
inline bool CppFeatures::legacy_closed_enum() const {
  // @@protoc_insertion_point(field_get:pb.CppFeatures.legacy_closed_enum)
  return _internal_legacy_closed_enum();
}
inline void CppFeatures::set_legacy_closed_enum(bool value) {
  _internal_set_legacy_closed_enum(value);
  _impl_._has_bits_[0] |= 0x00000001u;
  // @@protoc_insertion_point(field_set:pb.CppFeatures.legacy_closed_enum)
}
inline bool CppFeatures::_internal_legacy_closed_enum() const {
  PROTOBUF_TSAN_READ(&_impl_._tsan_detect_race);
  return _impl_.legacy_closed_enum_;
}
inline void CppFeatures::_internal_set_legacy_closed_enum(bool value) {
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  _impl_.legacy_closed_enum_ = value;
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif  // __GNUC__

// @@protoc_insertion_point(namespace_scope)
}  // namespace pb


// @@protoc_insertion_point(global_scope)

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_INCLUDED_google_2fprotobuf_2fcpp_5ffeatures_2eproto_2epb_2eh
