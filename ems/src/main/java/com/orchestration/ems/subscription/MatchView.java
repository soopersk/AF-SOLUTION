package com.orchestration.ems.subscription;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * The case-folded <b>matching view</b> (Amendment A15): every object key and every string value in the
 * activation tree, lower-cased, recursively.
 *
 * <p><b>Why this exists.</b> A13 established that CEL is case-sensitive in keys <em>and</em> values while
 * the legacy engine was case-insensitive in both. Without a fold somewhere, every rule has to carry the
 * difference itself — {@code .lowerAscii()} on both sides of each comparison, {@code has()}-guarded
 * either-spelling branches wherever the sources disagree on key case ({@code TYPE} vs {@code type},
 * {@code batchtype} vs {@code batchType}). That is unreadable, and these rules are authored and
 * maintained by DAG authors, not by this codebase. Folding once, here, buys exactly one rule dialect:
 * <b>all-lowercase paths, all-lowercase literals, plain {@code ==} and {@code &&}</b>.
 *
 * <p><b>What this is deliberately NOT.</b> It is a routing-path view only. The stored JSONB stays byte
 * verbatim, the promoted/generated columns are unaffected, and {@code /run/status}, {@code /gate/groups}
 * and the outgoing Airflow {@code conf} never see a folded tree. In particular it is <b>not</b> part of
 * {@link com.orchestration.ems.ingestion.Normalizer}: folding there would make
 * {@code ems_normalization_mutations_total} fire on ordinary traffic and destroy the
 * reviewed-and-approximately-zero meaning §4.4 depends on before cutover.
 *
 * <p><b>Hyphens (A18).</b> Case folding alone does not reconcile {@code run-category} with
 * {@code runCategory} — A8 established that both spellings are live. So every hyphenated key also gains
 * a hyphen-free alias, and a rule says {@code context.data.runcategory} once. Where a payload carries
 * both spellings the camelCase value wins; see {@link #addHyphenFreeAliases}.
 *
 * <p><b>Collisions.</b> Folding {@code TYPE} and {@code type} into one key loses one of them. The
 * survivor is the <b>last in document order</b>, which is what the legacy case-insensitive map lookup
 * did, and the collision is logged once per key pair so it is discoverable rather than silent.
 */
public final class MatchView {

    private static final Logger log = LoggerFactory.getLogger(MatchView.class);

    /** Key pairs already reported, so a collision on every event does not become a log flood. */
    private static final Set<String> REPORTED_COLLISIONS = ConcurrentHashMap.newKeySet();

    private MatchView() {
    }

    /**
     * Deep-copies {@code node} with every object key and every string value lower-cased. Numbers,
     * booleans and nulls are carried through untouched — folding them would change their type or their
     * meaning, and no rule compares against them case-insensitively.
     *
     * @param node the tree to fold; may be {@code null}
     * @return a new tree; the input is never mutated
     */
    public static JsonNode fold(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (node.isObject()) {
            return foldObject(node);
        }
        if (node.isArray()) {
            ArrayNode folded = JsonNodeFactory.instance.arrayNode(node.size());
            for (JsonNode element : node) {
                folded.add(fold(element));
            }
            return folded;
        }
        if (node.isTextual()) {
            return TextNode.valueOf(lower(node.textValue()));
        }
        return node.deepCopy();
    }

    private static JsonNode foldObject(JsonNode node) {
        ObjectNode folded = JsonNodeFactory.instance.objectNode();
        // properties() rather than the deprecated fields(); both preserve document order.
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            String key = lower(field.getKey());
            if (folded.has(key)) {
                reportCollision(field.getKey(), key);
            }
            // set() overwrites, so the LAST spelling in document order wins — legacy parity.
            folded.set(key, fold(field.getValue()));
        }
        addHyphenFreeAliases(folded);
        return folded;
    }

    /**
     * Second pass: every hyphenated key also becomes reachable without its hyphens, so
     * {@code reporting-date} and {@code reportingDate} are one name to a rule author (A18).
     *
     * <p><b>camelCase wins.</b> A camelCase spelling folds <em>directly</em> onto the hyphen-free name in
     * the first pass, and this pass never overwrites a key that is already there — so where a payload
     * carries both, the camelCase value is the one a rule sees, and the hyphenated spelling is the
     * fallback. That order is deliberate and is not document order: it must not depend on which spelling
     * a particular producer happens to emit first.
     *
     * <p>The hyphenated key is kept as well, so a rule written as {@code context.data["run-category"]}
     * still resolves. Nothing is removed from the view; aliases are only ever added.
     */
    private static void addHyphenFreeAliases(ObjectNode folded) {
        for (Map.Entry<String, JsonNode> field : List.copyOf(folded.properties())) {
            String key = field.getKey();
            if (key.indexOf('-') < 0) {
                continue;
            }
            String alias = key.replace("-", "");
            if (!alias.isEmpty() && !folded.has(alias)) {
                folded.set(alias, field.getValue());
            }
        }
    }

    /** {@link Locale#ROOT} explicitly: a Turkish default locale would fold {@code I} to a dotless i. */
    private static String lower(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private static void reportCollision(String originalKey, String foldedKey) {
        if (REPORTED_COLLISIONS.add(originalKey + "->" + foldedKey)) {
            log.warn("Subscription match view: key '{}' collides with an earlier spelling of '{}' after "
                    + "case folding; the later value wins (A15)", originalKey, foldedKey);
        }
    }
}
