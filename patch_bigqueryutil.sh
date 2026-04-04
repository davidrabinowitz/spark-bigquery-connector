sed -i '' -e '/import com.google.common.base.Preconditions;/a\
import java.lang.reflect.Field;\
import java.util.ArrayList;\
import java.util.List;\
import org.slf4j.Logger;\
import org.slf4j.LoggerFactory;\
' bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java
