use std::sync::Arc;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, path::Path as ObjectPath};
use url::Url;

/// Cache key for S3 stores that distinguishes between authenticated and anonymous access
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct S3CacheKey {
    bucket: String,
    anonymous: bool,
}

/// Global cache for S3 stores, keyed by (bucket, anonymous) tuple.
///
/// This cache prevents recreating S3 stores (which involves credential fetching
/// and potentially querying the EC2 metadata service) for the same bucket.
/// Uses DashMap for lock-free concurrent access.
///
/// # Credential Management
///
/// **✅ Automatic Credential Refresh:**
/// This implementation uses `AmazonS3Builder::from_env()` which leverages AWS's
/// built-in credential provider chain with automatic refresh for:
/// - **IAM Instance Roles (EC2)**: Credentials refresh automatically before expiration
/// - **IAM Task Roles (ECS)**: Container credentials refresh automatically
/// - **IAM Service Account Roles (EKS)**: Pod identity credentials refresh automatically
/// - **IAM Identity Center (SSO)**: Session tokens refresh when close to expiring
/// - **Assume Role credentials**: Refreshed automatically by AWS SDK
///
/// **⚠️ STS Temporary Credentials Limitation:**
/// If you're using short-lived STS credentials obtained via `AssumeRole` that are
/// NOT managed by the AWS SDK's credential provider chain (e.g., you're fetching
/// them yourself and passing them explicitly via `.with_access_key_id()`), cached
/// stores will NOT automatically refresh them. In that case, you would need to
/// implement cache expiration or store recreation logic.
///
/// **❌ NOT Supported (No Auto-Refresh):**
/// - Static access keys set via environment variables (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
///   These don't expire, so no refresh is needed, but rotating them requires restarting the app.
///
/// **Assumption:**
/// This code assumes you are running in an AWS environment with IAM roles or have
/// configured AWS credentials that support automatic refresh (SSO, assume role, etc.).
static S3_STORE_CACHE: Lazy<DashMap<S3CacheKey, Arc<dyn ObjectStore>>> =
    Lazy::new(DashMap::new);

/// Gets or creates a cached S3 store for the given bucket.
///
/// This function maintains a global cache of S3 stores to avoid the overhead
/// of recreating stores and refetching credentials for each operation. The
/// cache is thread-safe and uses lock-free concurrent access for high performance.
///
/// **Credential Refresh:** The underlying AWS SDK handles credential refresh
/// automatically for IAM roles, ECS tasks, EKS pods, and SSO tokens. The cached
/// store will continue to work as credentials are refreshed transparently.
///
/// # Arguments
///
/// * `bucket` - S3 bucket name (without "s3://" prefix)
/// * `anonymous` - If true, uses unsigned requests (for public buckets)
///
/// # Returns
///
/// Returns an `Arc<dyn ObjectStore>` for the specified S3 bucket. If a store
/// for this (bucket, anonymous) combination already exists in the cache, returns
/// the cached instance. Otherwise, creates a new store, caches it, and returns it.
///
/// # Errors
///
/// Returns an error if:
/// * AWS credentials cannot be found or are invalid (when anonymous=false)
/// * Bucket name is invalid
/// * Network connection to AWS fails during store creation
///
/// # Thread Safety
///
/// This function is thread-safe and lock-free. Multiple threads can safely
/// call this function concurrently without blocking each other (except for
/// rare cache misses on the same bucket).
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// # use keywords::utils::file_interaction_local_and_cloud::get_cached_s3_store;
///
/// // First call creates and caches the store
/// let store1 = get_cached_s3_store("globalnightlight", true).unwrap();
///
/// // Second call returns the cached store (fast, lock-free)
/// let store2 = get_cached_s3_store("globalnightlight", true).unwrap();
///
/// // Both are the same instance
/// assert!(Arc::ptr_eq(&store1, &store2));
///
/// // Different anonymous flag = different store
/// let store3 = get_cached_s3_store("globalnightlight", false).unwrap();
/// assert!(!Arc::ptr_eq(&store1, &store3));
/// ```
pub fn get_cached_s3_store(
    bucket: &str,
    anonymous: bool
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let cache_key = S3CacheKey {
        bucket: bucket.to_string(),
        anonymous,
    };
    let entry = S3_STORE_CACHE.entry(cache_key.clone());
    let store = entry.or_try_insert_with(|| {
        create_s3_store(bucket, anonymous)
    })?;
    Ok(Arc::clone(store.value()))
}

/// Creates an `ObjectStore` and path from a file path string.
///
/// This function provides a unified interface for accessing both local files
/// and S3 objects. It automatically detects the storage type based on the
/// file path format and returns an appropriate `ObjectStore` implementation.
///
/// **Performance Note**: S3 stores are automatically cached by bucket name,
/// so repeated calls with the same S3 bucket are efficient.
///
/// # Supported Path Formats
///
/// * **S3**: `"s3://bucket/key"` or `"s3://bucket/key?anon=true"` → Uses AWS S3 (cached by bucket)
/// * **Local**: Absolute or relative paths → Uses local filesystem
///   - Windows: `"C:\\path\\to\\file"` or relative paths
///   - Unix: `"/path/to/file"` or relative paths
///
/// # Arguments
///
/// * `file_path` - A string representing either an S3 URI or a local file path
///
/// # Returns
///
/// Returns `Ok((store, path))` where:
/// * `store` - An `Arc<dyn ObjectStore>` for accessing the storage backend
/// * `path` - An `ObjectPath` representing the file location within the store
///
/// # Errors
///
/// Returns an error if:
/// * S3 URL is malformed or missing bucket name
/// * AWS credentials cannot be found (for S3 paths on first access to a bucket)
/// * Local path is invalid (Windows only - missing drive letter)
/// * Current directory cannot be determined (for relative paths)
///
/// # Implementation Notes
///
/// * **S3 Caching**: S3 stores are cached globally by (bucket, anonymous) tuple.
///   The first access to a bucket creates the store (involves credential fetching
///   and potentially querying the EC2 metadata service). Subsequent accesses to
///   the same bucket with the same anonymous flag reuse the cached store.
/// * **Credentials**: For S3, uses AWS credential chain (environment variables,
///   credentials file, EC2 instance profile, ECS task role, EKS service account, SSO)
///   with automatic refresh for supported providers.
/// * **Path Normalization**: Converts Windows backslashes to forward slashes
///   and resolves relative paths to absolute paths
///
/// # Examples
///
/// ## Local file paths (testable)
///
/// ```no_run
/// # use keywords::utils::file_interaction_local_and_cloud::get_object_store;
/// # tokio_test::block_on(async {
/// // Local absolute path (Unix)
/// let (store, path) = get_object_store("/home/user/data/file.parquet").await.unwrap();
///
/// // Local absolute path (Windows)
/// let (store, path) = get_object_store("C:\\Users\\user\\data\\file.parquet").await.unwrap();
///
/// // Local relative path
/// let (store, path) = get_object_store("./data/file.parquet").await.unwrap();
/// # });
/// ```
///
/// ## S3 paths
///
/// ```no_run
/// # use keywords::utils::file_interaction_local_and_cloud::get_object_store;
/// # tokio_test::block_on(async {
/// // S3 path - first call creates and caches store
/// let (store, path) = get_object_store("s3://globalnightlight/201204/201204_catalog.json?anon=true").await.unwrap();
/// let bytes = store.get(&path).await.unwrap();
///
/// // S3 path - same bucket and anon flag, reuses cached store (fast!)
/// let (store2, path2) = get_object_store("s3://globalnightlight/201204/GDNBO_npp_d20120401_t0653006_e0658410_b02212_c20120428182646476060_devl_pop.li.co.tif?anon=true").await.unwrap();
///
/// // Different anon flag - creates separate cached store
/// let (store3, path3) = get_object_store("s3://globalnightlight/data.json").await.unwrap(); // anon=false
/// # });
/// ```
pub async fn get_object_store(
    file_path: &str,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), Box<dyn std::error::Error + Send + Sync>> {
    if file_path.starts_with("s3://") {
        let url = Url::parse(file_path)?;
        let bucket = url.host_str()
            .ok_or("Invalid S3 URL - no bucket specified")?;
        let key = url.path().trim_start_matches('/');

        // Check for anonymous access flag in query parameters
        let anonymous = url.query_pairs()
            .any(|(k, v)| k == "anon" && (v == "true" || v == "1"));

        // Use cached store - much faster than recreating
        let store = get_cached_s3_store(bucket, anonymous)?;
        let path = ObjectPath::from(key);

        Ok((store, path))
    } else {
        use std::path::Path as StdPath;

        let std_path = StdPath::new(file_path);
        let absolute_path = if std_path.is_absolute() {
            std_path.to_path_buf()
        } else {
            std::env::current_dir()?.join(std_path)
        };

        #[cfg(windows)]
        let (root, relative) = {
            let path_str = absolute_path.to_string_lossy();
            if let Some(pos) = path_str.find(":\\") {
                let root = format!("{}:\\", &path_str[..pos]);
                let relative = path_str[pos+2..].trim_start_matches('\\').replace('\\', "/");
                (root, relative)
            } else {
                return Err("Invalid Windows path".into());
            }
        };

        #[cfg(not(windows))]
        let (root, relative) = {
            let path_str = absolute_path.to_string_lossy();
            let relative = path_str.trim_start_matches('/');
            ("/".to_string(), relative.to_string())
        };

        let local_store = LocalFileSystem::new_with_prefix(root)?;
        let store: Arc<dyn ObjectStore> = Arc::new(local_store);
        let path = ObjectPath::from(relative);

        Ok((store, path))
    }
}

/// Creates a reusable S3 `ObjectStore` for a specific bucket.
///
/// This function creates an S3 store that can be reused for multiple operations
/// on the same bucket, avoiding the overhead of recreating stores and refetching
/// credentials for each operation.
///
/// **Note**: When using `get_object_store()` or `get_cached_s3_store()`, S3 stores
/// are automatically cached, so you typically don't need to call this function
/// directly unless you want explicit control over store creation.
///
/// # AWS Credential Chain with Automatic Refresh
///
/// Credentials are resolved using `from_env()` which checks (in order):
/// 1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
/// 2. **AWS credentials file**: `~/.aws/credentials`
/// 3. **EC2 instance profile**: Queries metadata service (credentials auto-refresh)
/// 4. **ECS task role**: For containers running in ECS (credentials auto-refresh)
/// 5. **EKS service account**: For pods in EKS (credentials auto-refresh)
/// 6. **SSO tokens**: From `aws sso login` (credentials auto-refresh)
///
/// The AWS SDK will automatically refresh credentials before they expire for
/// supported credential providers (IAM roles, ECS, EKS, SSO).
///
/// # Arguments
///
/// * `bucket` - S3 bucket name (without `"s3://"` prefix or trailing slashes)
/// * `anonymous` - If true, skips signing (for public buckets); if false, uses credentials
///
/// # Returns
///
/// Returns an `Arc<dyn ObjectStore>` that can be used to access objects in the
/// specified S3 bucket.
///
/// # Errors
///
/// Returns an error if:
/// * AWS credentials cannot be found or are invalid (when anonymous=false)
/// * Bucket name is invalid
/// * Network connection to AWS fails
///
/// # Examples
///
/// ```no_run
/// # use keywords::utils::file_interaction_local_and_cloud::create_s3_store;
/// use object_store::path::Path;
///
/// // Create a reusable S3 store
/// # tokio_test::block_on(async {
/// let store = create_s3_store("globalnightlight", true).unwrap();
///
/// // Use it for multiple operations - credentials auto-refresh
/// let file1 = store.get(&Path::from("201204/201204_catalog.json")).await.unwrap();
/// let file2 = store.get(&Path::from("201204/GDNBO_npp_d20120401_t0653006_e0658410_b02212_c20120428182646476060_devl_pop.li.co.tif")).await.unwrap();
/// # })
/// ```
pub fn create_s3_store(
    bucket: &str,
    anonymous: bool
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket);

    if anonymous {
        // Skip credential loading and request signing for public buckets
        builder = builder.with_skip_signature(true);
    }

    let s3_store = builder.build()?;
    Ok(Arc::new(s3_store))
}

/// Creates a reusable local filesystem `ObjectStore`.
///
/// This function returns an `ObjectStore` that provides access to the entire
/// local filesystem. The store can be reused for multiple file operations.
///
/// # Returns
///
/// Returns an `Arc<dyn ObjectStore>` for accessing local files through the
/// `ObjectStore` interface.
///
/// # Examples
///
/// ```
/// use object_store::ObjectStore;
/// use std::sync::Arc;
/// # use keywords::utils::file_interaction_local_and_cloud::create_local_store;
///
/// // Create a local filesystem store
/// let store = create_local_store();
///
/// // Returns an Arc<dyn ObjectStore>
/// assert!(Arc::strong_count(&store) == 1);
/// ```
///
/// # Notes
///
/// * The returned store has access to the entire filesystem (no root prefix)
/// * Use appropriate paths based on your operating system conventions
/// * For restricted access to a specific directory, use `LocalFileSystem::new_with_prefix()`
pub fn create_local_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}