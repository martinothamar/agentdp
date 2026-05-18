# Refactoring and semantic compression

Architecture issues:

- cli <-> server request response is unstructured and poor. The protocol should specify request/response/events as structured serde_json serializable enums. The request body/value should be this Value with method as descriminator as opposed to unstructured json serde_json::Value. Same structured pattern for responses. Then when extracting fields from the response, we should simply be able to access fields intead of doing the cumbersome string_field(...) methods (they should be removed)
- the protocol should also encapsulate NEXT_COMMAND_ID as part of a RequestFactory or something, so that it is properly encapsulated and doesnt leak into cli/frontend layers
- the request object should just have a respond_with(...)/respond_with_success(...)/respond_with_failure(...) method(s) that automatically maps over the request ID ideally to make this smooth
- a lot of tests assert on the form and output of large structs, but this is why we have snapshot testing. Manual assertions are only OK in the cases where we have 1-5 fields to assert the values for. Anything larger than that should just be snapshots that can be reviewed as diff when changed, then we dont have to refactor assertions and can merely review the snapshot diffs when we make changes
- doctor and some other code is currently platform-oblivious. Doctor for example checks linux-specific APIs and binaries
  - there is no point in check_kvm on anything other than linux
  - qemu-image is also linux AND backend-specific
  - curl is platform-specific and is an unnecessary dependency leaking out into docker. Our primary need here is to download something over HTTP, and AFAICT the only thing that needs that is ensuring we have the necessary image as part of the QEMU backend. For deps/tools/needs that are VM backend-specific, subsets of doctor checks should be delegated to the backend. Which means that the type of backend should probably be resolved early and put into context along with the running platform (platform::host_target())
  - same goes for ssh, there may be backends that dont require ssh so it should not be required in general.
- I believe SshKeygen is OS-specific, it should probably reside in platform module and provide a single stable interfaces that normalizes OS-specific behavior (it for sure is a little different on Windows for example)
- the qemu module is the right approach, but it should probably be exposed as a trait implementation to more strongly encapsulate what today is qemu-specific (which should be a lot right now I imagine). If we get more backends later we can extract shared functionality into conceptual modules. Like ssh for example is a good candidate for this. I believe bootstrap generation is also something that should be independent, as the shell/linux environmment is going to be very similar across backends.
- instance.rs should probably become a instance/ folder module and instance.rs should be split into multiple files according to behavior. lock.rs should probably also be moved into the instance.rs. The module should expose a single struct Instance I believe which should have both creation APIs and load APIs, and optionally explicity lock/lockguard APIs if that is necessary
- some of the logic now in agentdp-server should probably be moved into agentdp-core? Server is meant to be the local runtime, in the sense that we may have a second runtime called agentdp-operator which would be the k8s operator server equivalent. So server is on the runtime/executor level which should know about the runtime environmment, whereas agentdp-core should not

It is important that we get the boundaries and structure right, dont let internals and implementations details leak outside where it is absolutely necessary.


Compression:
- a lot of similar test setup exists, i.e. setting environment variables. There is room for extraction into test support/fixture code here for common cases I believe
- large number of single line funcctions such as params_from_value, consider whether any of these should just be inlined unless they encode member fields as part of the single line call or similar
- PlatformPaths could probably also be collapsed into Context as it is required essentially everywhere


When refactoring and compressing, it is important that we sill keep the same outputs: for now an agent qemu VM adhering to the YAML spec (in the current implementation form as of last manual testing iteration, not everything is implemented yet).
