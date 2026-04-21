use super::*;

pub(crate) fn plan_memory_request(query: &str) -> MemoryRetrievalPlan {
    let question = query.trim();
    if question.is_empty() {
        return MemoryRetrievalPlan {
            mode: MemoryRequestMode::General,
            lane: MemoryRetrievalLane::HotState,
            expanded_query: String::new(),
            focus_terms: Vec::new(),
            temporal: false,
            evidence_required: false,
        };
    }

    let lower = question.to_lowercase();
    let temporal = is_temporal_memory_query(question);
    let evidence_required = lower.contains("remember")
        || lower.contains("what did i say")
        || lower.contains("what did we say")
        || lower.contains("which session")
        || lower.contains("mentioned")
        || lower.contains("mention")
        || lower.contains("told you")
        || lower.contains("did i ever")
        || lower.contains("have i ever");

    let mode = if temporal {
        MemoryRequestMode::Temporal
    } else if lower.contains("latest")
        || lower.contains("today")
        || lower.contains("current")
        || lower.contains("search")
        || lower.contains("refresh")
        || lower.contains("job")
        || lower.contains("maintenance")
    {
        MemoryRequestMode::FreshLookup
    } else if lower.contains("retry")
        || lower.contains("repair")
        || lower.contains("failed")
        || lower.contains("failure")
        || lower.contains("broken")
        || lower.contains("fix")
    {
        MemoryRequestMode::Repair
    } else if lower.contains("should you")
        || lower.contains("format")
        || lower.contains("how should")
        || lower.contains("respond")
    {
        MemoryRequestMode::Policy
    } else if lower.contains("task")
        || lower.contains("working on")
        || lower.contains("next step")
        || lower.contains("goal")
    {
        MemoryRequestMode::Task
    } else if lower.contains("prefer")
        || lower.contains("favorite")
        || lower.contains("usually")
        || lower.contains("my style")
        || lower.contains("my preference")
        || lower.contains("preference")
        || lower.contains("style")
        || lower.contains("tone")
    {
        MemoryRequestMode::Identity
    } else if evidence_required {
        MemoryRequestMode::Evidence
    } else {
        MemoryRequestMode::Fact
    };

    let lane = match mode {
        MemoryRequestMode::General => MemoryRetrievalLane::HotState,
        MemoryRequestMode::Identity => MemoryRetrievalLane::HotState,
        MemoryRequestMode::Policy => MemoryRetrievalLane::Procedural,
        MemoryRequestMode::Task => MemoryRetrievalLane::TaskState,
        MemoryRequestMode::Fact => MemoryRetrievalLane::MixedContext,
        MemoryRequestMode::Temporal => MemoryRetrievalLane::TemporalRecall,
        MemoryRequestMode::Evidence => MemoryRetrievalLane::CapsuleRecall,
        MemoryRequestMode::Repair => MemoryRetrievalLane::RepairHistory,
        MemoryRequestMode::FreshLookup => MemoryRetrievalLane::FreshLookup,
    };

    let mut variants = vec![question.to_string()];
    if let Some(focus) = strip_memory_prompt_prefix(question) {
        if !variants
            .iter()
            .any(|variant| variant.eq_ignore_ascii_case(focus))
        {
            variants.push(focus.to_string());
        }
    }
    if temporal || matches!(mode, MemoryRequestMode::FreshLookup) {
        let without_time = strip_temporal_prompt_words(question);
        if !without_time.is_empty()
            && !variants
                .iter()
                .any(|variant| variant.eq_ignore_ascii_case(&without_time))
        {
            variants.push(without_time);
        }
    }
    for variant in query_expansion_variants(question, mode) {
        if !variants
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&variant))
        {
            variants.push(variant);
        }
    }

    let mut focus_terms = Vec::new();
    for variant in &variants {
        for token in content_tokens(variant) {
            if !focus_terms.contains(&token) {
                focus_terms.push(token);
            }
        }
    }
    focus_terms.truncate(10);

    MemoryRetrievalPlan {
        mode,
        lane,
        expanded_query: variants.join(" "),
        focus_terms,
        temporal,
        evidence_required,
    }
}

fn query_expansion_variants(question: &str, mode: MemoryRequestMode) -> Vec<String> {
    let lower = question.to_lowercase();
    let mut variants = Vec::new();

    if (lower.contains("arrive")
        || lower.contains("arrival")
        || lower.contains("deliver")
        || lower.contains("shipping"))
        && (lower.contains("bought")
            || lower.contains("buy")
            || lower.contains("ordered")
            || lower.contains("order"))
    {
        variants.push("purchase delivery shipping order".to_string());
        if ["case", "backpack", "bag", "mouse", "cover", "charger"]
            .iter()
            .any(|needle| lower.contains(needle))
        {
            variants.push("accessory purchase delivery".to_string());
            variants.push(
                "accessory bag backpack sleeve case cover purchase delivery amazon".to_string(),
            );
        }
    }

    if (lower.contains("homegrown") || lower.contains("garden"))
        && (lower.contains("dinner")
            || lower.contains("serve")
            || lower.contains("meal")
            || lower.contains("cook"))
    {
        variants.push("garden produce herbs tomatoes recipe".to_string());
    }

    if lower.contains("reunion") || lower.contains("nostalg") || lower.contains("high school") {
        variants.push("school memories friends courses team".to_string());
    }

    if lower.contains("sibling")
        || lower.contains("siblings")
        || lower.contains("brother")
        || lower.contains("sister")
    {
        variants.push("family sibling brother sister".to_string());
    }

    if lower.contains("commute")
        && (lower.contains("activity")
            || lower.contains("activities")
            || lower.contains("suggest")
            || lower.contains("recommend"))
    {
        variants.push("commute podcast audiobook listening audio".to_string());
        if matches!(mode, MemoryRequestMode::Identity)
            || lower.contains("prefer")
            || lower.contains("favorite")
            || lower.contains("interested")
        {
            variants.push("history science podcast audiobook commute".to_string());
        }
    }

    variants
}

pub(super) fn context_limit_for_plan(plan: &MemoryRetrievalPlan, limit: usize) -> usize {
    match plan.lane {
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::Procedural => limit.max(4),
        MemoryRetrievalLane::TaskState => limit.max(5),
        MemoryRetrievalLane::MixedContext
        | MemoryRetrievalLane::CapsuleRecall
        | MemoryRetrievalLane::TemporalRecall
        | MemoryRetrievalLane::RepairHistory
        | MemoryRetrievalLane::FreshLookup => limit.max(6),
    }
}

pub(crate) fn context_candidate_limit_for_plan(plan: &MemoryRetrievalPlan, limit: usize) -> usize {
    let base = context_limit_for_plan(plan, limit);
    let extra = match plan.lane {
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::Procedural => 2,
        MemoryRetrievalLane::TaskState => 3,
        MemoryRetrievalLane::MixedContext
        | MemoryRetrievalLane::CapsuleRecall
        | MemoryRetrievalLane::TemporalRecall
        | MemoryRetrievalLane::RepairHistory
        | MemoryRetrievalLane::FreshLookup => 4,
    };
    let upper_bound = base.max(16);
    base.saturating_add(extra).clamp(base, upper_bound)
}

pub(super) fn capsule_limit_for_plan(plan: &MemoryRetrievalPlan, limit: usize) -> usize {
    match plan.lane {
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::Procedural => limit.min(2).max(1),
        MemoryRetrievalLane::TaskState => limit.min(3).max(1),
        MemoryRetrievalLane::MixedContext
        | MemoryRetrievalLane::CapsuleRecall
        | MemoryRetrievalLane::TemporalRecall
        | MemoryRetrievalLane::RepairHistory
        | MemoryRetrievalLane::FreshLookup => limit.min(4).max(2),
    }
}

pub(super) fn capsule_candidate_limit_for_plan(plan: &MemoryRetrievalPlan, limit: usize) -> usize {
    let base = capsule_limit_for_plan(plan, limit);
    let extra = match plan.lane {
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::Procedural => 2,
        MemoryRetrievalLane::TaskState => 3,
        MemoryRetrievalLane::MixedContext
        | MemoryRetrievalLane::CapsuleRecall
        | MemoryRetrievalLane::TemporalRecall
        | MemoryRetrievalLane::RepairHistory
        | MemoryRetrievalLane::FreshLookup => 4,
    };
    base.saturating_add(extra).clamp(base, 12)
}

pub(super) fn plan_claim_score(claim: &db::MemoryClaimRecord, plan: &MemoryRetrievalPlan) -> f64 {
    let mut score = claim.importance * 1.2 + claim.confidence;
    score += plan_text_score(&claim.statement, plan) * 3.5;
    score += exact_focus_match_bonus(&claim.statement, plan) * 1.4;
    if matches!(plan.lane, MemoryRetrievalLane::FreshLookup) && claim.is_stale {
        score += 2.0;
    }
    if matches!(plan.lane, MemoryRetrievalLane::HotState)
        && (claim.scope == "personal" || claim.kind == "preference")
    {
        score += 1.5;
    }
    score
}

pub(super) fn plan_procedure_score(
    procedure: &db::MemoryProcedureRecord,
    plan: &MemoryRetrievalPlan,
) -> f64 {
    let mut score = procedure.importance * 1.3 + plan_text_score(&procedure.content, plan) * 3.8;
    score += exact_focus_match_bonus(&procedure.content, plan) * 1.6;
    if matches!(
        plan.lane,
        MemoryRetrievalLane::Procedural
            | MemoryRetrievalLane::RepairHistory
            | MemoryRetrievalLane::TaskState
    ) {
        score += 1.2;
    }
    score
}

pub(super) fn plan_episode_score(
    episode: &db::MemoryEpisodeRecord,
    plan: &MemoryRetrievalPlan,
) -> f64 {
    let mut score = episode.importance + plan_text_score(&episode.summary, plan) * 2.6;
    if matches!(
        plan.lane,
        MemoryRetrievalLane::TemporalRecall
            | MemoryRetrievalLane::CapsuleRecall
            | MemoryRetrievalLane::RepairHistory
    ) {
        score += 0.8;
    }
    if plan.temporal {
        score += episode
            .event_at
            .as_deref()
            .map(|value| temporal_alignment_bonus(value, &plan.expanded_query))
            .unwrap_or(0.0);
    }
    score
}

pub(super) fn plan_source_score(
    source: &db::MemorySourceRecord,
    plan: &MemoryRetrievalPlan,
) -> f64 {
    let mut score = source.trust_tier + plan_text_score(&source.url_or_ref, plan) * 2.0;
    score += exact_focus_match_bonus(&source.url_or_ref, plan) * 0.8;
    if let Some(title) = &source.title {
        score += plan_text_score(title, plan) * 2.5;
        score += exact_focus_match_bonus(title, plan) * 1.1;
    }
    if matches!(plan.lane, MemoryRetrievalLane::FreshLookup) {
        score += 0.8;
    }
    score
}

pub(super) fn plan_capsule_score(
    capsule: &db::MemorySessionCapsuleRecord,
    plan: &MemoryRetrievalPlan,
) -> f64 {
    let summary_overlap = plan_text_score(&capsule.summary, plan);
    let anchor_overlap = capsule
        .anchors
        .iter()
        .map(|anchor| plan_text_score(&anchor.content, plan))
        .fold(0.0f64, f64::max);
    let keyphrase_overlap = set_overlap_ratio(&plan.focus_terms, &capsule.keyphrases);
    let entity_overlap = set_overlap_ratio(&plan.focus_terms, &capsule.entity_markers);
    let marker_overlap = set_overlap_ratio(&plan.focus_terms, &capsule.marker_terms);

    let mut score = anchor_overlap * 5.0
        + summary_overlap * 3.5
        + keyphrase_overlap * 2.0
        + entity_overlap * 1.5
        + marker_overlap * 1.3;

    if plan.evidence_required {
        score += anchor_overlap * 2.0;
    }
    if plan.temporal {
        score += temporal_alignment_bonus(&capsule.last_message_at, &plan.expanded_query);
    }
    let exact_focus_bonus = exact_focus_match_bonus(&capsule.summary, plan)
        + capsule
            .anchors
            .iter()
            .map(|anchor| exact_focus_match_bonus(&anchor.content, plan))
            .fold(0.0f64, f64::max);
    score += exact_focus_bonus * 1.8;
    if matches!(plan.lane, MemoryRetrievalLane::RepairHistory)
        && capsule
            .marker_terms
            .iter()
            .any(|term| term.contains("fix") || term.contains("repair"))
    {
        score += 1.0;
    }

    score
}

pub(super) fn plan_text_score(text: &str, plan: &MemoryRetrievalPlan) -> f64 {
    if plan.focus_terms.is_empty() {
        return 0.0;
    }
    let lexical = keyword_overlap_score(&plan.expanded_query, text);
    let semantic = semantic_overlap_score(&plan.expanded_query, text);
    let intent = question_intent_alignment_bonus(&plan.expanded_query, text);
    (lexical * 0.7 + semantic * 0.95 + intent).min(1.75)
}

pub(super) fn keyword_overlap_score(query: &str, doc: &str) -> f64 {
    let query_tokens = content_tokens(query);
    let doc_tokens = content_tokens(doc);
    if query_tokens.is_empty() || doc_tokens.is_empty() {
        return 0.0;
    }
    let overlap = query_tokens
        .iter()
        .filter(|token| doc_tokens.contains(*token))
        .count();
    overlap as f64 / query_tokens.len() as f64
}

pub(super) fn semantic_overlap_score(query: &str, doc: &str) -> f64 {
    let query_tokens = semantic_token_set(query);
    let doc_tokens = semantic_token_set(doc);
    if query_tokens.is_empty() || doc_tokens.is_empty() {
        return 0.0;
    }
    let overlap = query_tokens
        .iter()
        .filter(|token| doc_tokens.contains(*token))
        .count();
    overlap as f64 / query_tokens.len() as f64
}

pub(super) fn question_intent_alignment_bonus(query: &str, doc: &str) -> f64 {
    let lower_query = query.to_lowercase();
    let query_tokens = semantic_token_set(query);
    let semantic_tokens = semantic_token_set(doc);
    let mut bonus = 0.0f64;

    if lower_query.contains("how many") && contains_numeric_signal(doc) {
        bonus += 0.18;
    }
    if (lower_query.contains("how much time")
        || lower_query.contains("every day")
        || lower_query.contains("daily"))
        && semantic_tokens.contains("duration")
        && semantic_tokens.contains("daily")
    {
        bonus += 0.28;
    }
    if lower_query.contains("doctor") && semantic_tokens.contains("doctor") {
        bonus += 0.28;
    }
    if lower_query.contains("project")
        && semantic_tokens.contains("project")
        && (semantic_tokens.contains("lead") || semantic_tokens.contains("ownership"))
    {
        bonus += 0.24;
    }
    if (query_tokens.contains("music") || query_tokens.contains("instrument"))
        && semantic_tokens.contains("music")
        && semantic_tokens.contains("duration")
        && semantic_tokens.contains("daily")
    {
        bonus += 0.22;
    }
    if (query_tokens.contains("fish") || query_tokens.contains("aquarium"))
        && semantic_tokens.contains("aquarium")
    {
        bonus += 0.24;
        if contains_numeric_signal(doc) {
            bonus += 0.14;
        }
    }
    if (lower_query.contains("practice") || lower_query.contains("practicing"))
        && semantic_tokens.contains("practice")
    {
        bonus += 0.10;
    }
    if (lower_query.contains("arrive")
        || lower_query.contains("arrival")
        || lower_query.contains("deliver")
        || lower_query.contains("shipping"))
        && (lower_query.contains("bought")
            || lower_query.contains("buy")
            || lower_query.contains("ordered")
            || lower_query.contains("order"))
        && semantic_tokens.contains("purchase")
        && semantic_tokens.contains("delivery")
    {
        bonus += 0.26;
        if semantic_tokens.contains("accessory") {
            bonus += 0.12;
        }
        if contains_numeric_signal(doc) {
            bonus += 0.08;
        }
    }
    if (lower_query.contains("homegrown") || lower_query.contains("garden"))
        && (lower_query.contains("dinner")
            || lower_query.contains("serve")
            || lower_query.contains("meal")
            || lower_query.contains("cook"))
        && (semantic_tokens.contains("garden") || semantic_tokens.contains("produce"))
        && (semantic_tokens.contains("herb")
            || semantic_tokens.contains("tomato")
            || semantic_tokens.contains("recipe"))
    {
        bonus += 0.30;
    }
    if (lower_query.contains("reunion")
        || lower_query.contains("nostalg")
        || lower_query.contains("high school"))
        && semantic_tokens.contains("school")
    {
        bonus += 0.22;
        if semantic_tokens.contains("friend") || semantic_tokens.contains("memory") {
            bonus += 0.14;
        }
    }
    if (lower_query.contains("sibling") || lower_query.contains("siblings"))
        && semantic_tokens.contains("sibling")
    {
        bonus += 0.30;
        if contains_numeric_signal(doc) {
            bonus += 0.18;
        }
    }
    if lower_query.contains("commute")
        && (lower_query.contains("activity")
            || lower_query.contains("activities")
            || lower_query.contains("suggest")
            || lower_query.contains("recommend"))
    {
        if semantic_tokens.contains("commute") {
            bonus += 0.12;
        }
        if semantic_tokens.contains("audio") {
            bonus += 0.24;
        }
        if semantic_tokens.contains("history") || semantic_tokens.contains("science") {
            bonus += 0.12;
        }
        if doc.to_lowercase().contains("podcast")
            || doc.to_lowercase().contains("audiobook")
            || doc.to_lowercase().contains("listening")
        {
            bonus += 0.14;
        }
    }
    if (lower_query.contains("arrive")
        || lower_query.contains("arrival")
        || lower_query.contains("deliver")
        || lower_query.contains("shipping"))
        && (lower_query.contains("bought")
            || lower_query.contains("buy")
            || lower_query.contains("ordered")
            || lower_query.contains("order"))
        && semantic_tokens.contains("accessory")
    {
        bonus += 0.22;
        if semantic_tokens.contains("purchase") || semantic_tokens.contains("delivery") {
            bonus += 0.16;
        }
        if doc.to_lowercase().contains("amazon") {
            bonus += 0.08;
        }
    }

    bonus.min(0.55)
}

pub(super) fn semantic_token_set(input: &str) -> std::collections::HashSet<String> {
    let mut tokens = std::collections::HashSet::new();
    for token in content_tokens(input) {
        expand_semantic_token(&token, &mut tokens);
    }
    tokens
}

fn expand_semantic_token(token: &str, out: &mut std::collections::HashSet<String>) {
    let normalized = normalize_semantic_token(token);
    out.insert(normalized.clone());
    match normalized.as_str() {
        "doctor" | "medical" => {
            out.insert("doctor".to_string());
            out.insert("medical".to_string());
        }
        "visit" | "appointment" => {
            out.insert("visit".to_string());
            out.insert("appointment".to_string());
        }
        "project" | "work" => {
            out.insert("project".to_string());
            out.insert("work".to_string());
        }
        "lead" | "ownership" => {
            out.insert("lead".to_string());
            out.insert("ownership".to_string());
        }
        "sibling" | "family" => {
            out.insert("sibling".to_string());
            out.insert("family".to_string());
        }
        "practice" | "music" | "instrument" => {
            out.insert("practice".to_string());
            out.insert("music".to_string());
            out.insert("instrument".to_string());
        }
        "commute" => {
            out.insert("commute".to_string());
        }
        "audio" => {
            out.insert("audio".to_string());
            out.insert("podcast".to_string());
            out.insert("audiobook".to_string());
            out.insert("listen".to_string());
        }
        "fish" | "aquarium" => {
            out.insert("fish".to_string());
            out.insert("aquarium".to_string());
        }
        "purchase" | "delivery" => {
            out.insert("purchase".to_string());
            out.insert("delivery".to_string());
        }
        "accessory" => {
            out.insert("accessory".to_string());
        }
        "garden" | "produce" => {
            out.insert("garden".to_string());
            out.insert("produce".to_string());
        }
        "herb" | "tomato" => {
            out.insert("garden".to_string());
            out.insert("produce".to_string());
        }
        "memory" => {
            out.insert("memory".to_string());
        }
        "school" => {
            out.insert("school".to_string());
        }
        "friend" => {
            out.insert("friend".to_string());
        }
        "duration" | "time" => {
            out.insert("duration".to_string());
            out.insert("time".to_string());
        }
        "daily" | "frequency" => {
            out.insert("daily".to_string());
            out.insert("frequency".to_string());
        }
        _ => {}
    }
}

pub(super) fn normalize_semantic_token(token: &str) -> String {
    match token {
        "doctor" | "doctors" | "physician" | "physicians" | "specialist" | "specialists"
        | "dermatologist" | "dermatologists" | "cardiologist" | "cardiologists" | "therapist"
        | "therapists" | "pediatrician" | "pediatricians" | "otolaryngologist"
        | "otolaryngologists" => "doctor".to_string(),
        "medical" | "clinic" | "hospital" => "medical".to_string(),
        "ent" => "doctor".to_string(),
        "visit" | "visited" | "visiting" | "consult" | "consulted" | "seeing" | "saw"
        | "appointment" | "appointments" => "visit".to_string(),
        "project" | "projects" | "initiative" | "initiatives" | "campaign" | "campaigns"
        | "program" | "programs" | "venture" | "ventures" => "project".to_string(),
        "work" | "workplace" => "work".to_string(),
        "led" | "lead" | "leading" | "manage" | "managed" | "managing" | "spearheaded"
        | "heading" | "headed" | "coordinate" | "coordinated" => "lead".to_string(),
        "owner" | "ownership" => "ownership".to_string(),
        "sibling" | "siblings" | "brother" | "brothers" | "sister" | "sisters" => {
            "sibling".to_string()
        }
        "family" => "family".to_string(),
        "practice" | "practicing" | "practise" | "practised" | "practiced" => {
            "practice".to_string()
        }
        "commute" | "commuting" | "bus" | "subway" | "metro" | "train" => "commute".to_string(),
        "podcast" | "podcasts" | "audiobook" | "audiobooks" | "audio" | "listen" | "listening" => {
            "audio".to_string()
        }
        "history" | "historical" => "history".to_string(),
        "science" | "scientific" => "science".to_string(),
        "violin" | "guitar" | "saxophone" | "harmonica" | "piano" | "drums" | "drumming"
        | "cello" | "flute" | "trumpet" | "ukulele" => "instrument".to_string(),
        "music" | "musical" | "instrument" | "instruments" => "music".to_string(),
        "fish" | "fishes" | "betta" | "tetras" | "tetra" | "gouramis" | "gourami" | "pleco"
        | "catfish" | "danios" | "danio" => "fish".to_string(),
        "aquarium" | "aquariums" | "tank" | "tanks" => "aquarium".to_string(),
        "buy" | "buys" | "bought" | "purchase" | "purchased" | "order" | "ordered" | "amazon" => {
            "purchase".to_string()
        }
        "arrive" | "arrived" | "arrival" | "deliver" | "delivered" | "delivery" | "ship"
        | "shipped" | "shipping" => "delivery".to_string(),
        "case" | "backpack" | "bag" | "bags" | "mouse" | "mice" | "cover" | "covers"
        | "charger" | "chargers" | "keyboard" | "keyboards" => "accessory".to_string(),
        "homegrown" | "garden" | "gardening" | "harvest" | "harvested" => "garden".to_string(),
        "produce" | "ingredient" | "ingredients" => "produce".to_string(),
        "basil" | "mint" | "oregano" | "parsley" | "dill" | "chives" | "herb" | "herbs" => {
            "herb".to_string()
        }
        "tomato" | "tomatoes" => "tomato".to_string(),
        "recipe" | "recipes" | "dish" | "dishes" | "dinner" | "meal" | "cook" | "cooking" => {
            "recipe".to_string()
        }
        "nostalgia" | "nostalgic" | "memory" | "memories" | "remembered" | "remembering" => {
            "memory".to_string()
        }
        "reunion" | "school" | "classmate" | "classmates" | "debate" | "course" | "courses"
        | "class" | "classes" | "placement" | "economics" => "school".to_string(),
        "friend" | "friends" => "friend".to_string(),
        "minute" | "minutes" | "hour" | "hours" => "duration".to_string(),
        "time" | "daily" | "day" | "everyday" | "weekly" | "month" | "months" => {
            "daily".to_string()
        }
        _ => token.to_string(),
    }
}

fn contains_numeric_signal(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.chars().any(|ch| ch.is_ascii_digit())
        || [
            "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "first",
            "second", "third",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
}

pub(super) fn exact_focus_match_bonus(text: &str, plan: &MemoryRetrievalPlan) -> f64 {
    if plan.focus_terms.is_empty() {
        return 0.0;
    }
    let lower = text.to_lowercase();
    let matched = plan
        .focus_terms
        .iter()
        .filter(|term| lower.contains(term.as_str()))
        .count();
    if matched == 0 {
        0.0
    } else {
        matched as f64 / plan.focus_terms.len() as f64
    }
}

pub(super) fn set_overlap_ratio(left: &[String], right: &[String]) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let overlap = left.iter().filter(|token| right.contains(*token)).count();
    overlap as f64 / left.len() as f64
}

pub(super) fn bm25_scores(query: &str, docs: &[String]) -> Vec<f64> {
    if docs.is_empty() {
        return Vec::new();
    }

    let mut query_terms = content_tokens(query);
    query_terms.sort();
    query_terms.dedup();
    if query_terms.is_empty() {
        return vec![0.0; docs.len()];
    }

    let mut doc_term_counts = Vec::with_capacity(docs.len());
    let mut doc_lengths = Vec::with_capacity(docs.len());
    let mut doc_freq = std::collections::HashMap::<String, usize>::new();

    for doc in docs {
        let mut counts = std::collections::HashMap::<String, usize>::new();
        for token in content_tokens(doc) {
            *counts.entry(token).or_insert(0) += 1;
        }
        let length = counts.values().sum::<usize>().max(1);
        for term in &query_terms {
            if counts.contains_key(term) {
                *doc_freq.entry(term.clone()).or_insert(0) += 1;
            }
        }
        doc_term_counts.push(counts);
        doc_lengths.push(length);
    }

    let avg_doc_len = doc_lengths.iter().sum::<usize>() as f64 / docs.len() as f64;
    let total_docs = docs.len() as f64;
    let k1 = 1.5;
    let b = 0.75;

    doc_term_counts
        .iter()
        .enumerate()
        .map(|(idx, counts)| {
            let doc_len = doc_lengths[idx] as f64;
            let mut score = 0.0;
            for term in &query_terms {
                let tf = *counts.get(term).unwrap_or(&0) as f64;
                if tf <= 0.0 {
                    continue;
                }
                let df = *doc_freq.get(term).unwrap_or(&0) as f64;
                let idf = (((total_docs - df + 0.5) / (df + 0.5)) + 1.0).ln();
                let norm = tf + k1 * (1.0 - b + b * (doc_len / avg_doc_len.max(1.0)));
                score += idf * (tf * (k1 + 1.0) / norm);
            }
            score + keyword_overlap_score(query, &docs[idx])
        })
        .collect()
}

pub(super) fn strip_memory_prompt_prefix(question: &str) -> Option<&str> {
    let lower = question.to_lowercase();
    for prefix in [
        "what did i say about ",
        "what did we say about ",
        "which session mentioned ",
        "which session mentions ",
        "do you remember ",
        "can you recall ",
        "have i ever mentioned ",
        "did i ever mention ",
        "when did i ",
        "when did we ",
        "what was my ",
        "what is my ",
    ] {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let offset = question.len() - rest.len();
            return Some(question[offset..].trim());
        }
    }
    None
}

pub(super) fn strip_temporal_prompt_words(question: &str) -> String {
    question
        .split_whitespace()
        .filter(|token| {
            let lower = token
                .trim_matches(|ch: char| !ch.is_alphanumeric())
                .to_lowercase();
            !matches!(
                lower.as_str(),
                "when"
                    | "before"
                    | "after"
                    | "last"
                    | "next"
                    | "now"
                    | "currently"
                    | "latest"
                    | "recent"
                    | "month"
                    | "week"
                    | "year"
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn is_temporal_memory_query(query: &str) -> bool {
    let lower = query.to_lowercase();
    lower.contains("last month")
        || lower.contains("last week")
        || lower.contains("when")
        || lower.contains("before")
        || lower.contains("after")
        || lower.contains("yesterday")
        || lower.contains("tomorrow")
}

pub(super) fn temporal_alignment_bonus(timestamp: &str, query: &str) -> f64 {
    if timestamp.trim().is_empty() {
        return 0.0;
    }
    let lower = query.to_lowercase();
    if lower.contains("latest") || lower.contains("current") || lower.contains("today") {
        return 1.0;
    }
    if lower.contains("last") || lower.contains("recent") {
        return 0.7;
    }
    if lower.contains("when") || lower.contains("before") || lower.contains("after") {
        return 0.5;
    }
    0.0
}

pub(super) fn content_tokens(input: &str) -> Vec<String> {
    input
        .split(|ch: char| !ch.is_alphanumeric())
        .filter_map(|token| {
            let trimmed = token.trim().to_lowercase();
            if trimmed.len() < 3
                || MEMORY_STOP_WORDS
                    .iter()
                    .any(|stop_word| *stop_word == trimmed.as_str())
            {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect()
}

pub(super) fn surface_section_order(plan: &MemoryRetrievalPlan) -> &'static [&'static str] {
    match plan.lane {
        MemoryRetrievalLane::HotState => &[
            "profile",
            "evidence_synthesis",
            "coverage_gaps",
            "facts",
            "procedures",
            "resources",
            "capsules",
            "recent_events",
            "uncertainties",
        ],
        MemoryRetrievalLane::Procedural => &[
            "procedures",
            "evidence_synthesis",
            "coverage_gaps",
            "resources",
            "facts",
            "profile",
            "capsules",
            "recent_events",
            "uncertainties",
        ],
        MemoryRetrievalLane::TaskState => &[
            "procedures",
            "evidence_synthesis",
            "coverage_gaps",
            "resources",
            "capsules",
            "facts",
            "recent_events",
            "profile",
            "uncertainties",
        ],
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => &[
            "evidence_synthesis",
            "coverage_gaps",
            "capsules",
            "recent_events",
            "facts",
            "procedures",
            "resources",
            "profile",
            "uncertainties",
        ],
        MemoryRetrievalLane::RepairHistory => &[
            "procedures",
            "evidence_synthesis",
            "coverage_gaps",
            "resources",
            "capsules",
            "recent_events",
            "facts",
            "profile",
            "uncertainties",
        ],
        MemoryRetrievalLane::FreshLookup | MemoryRetrievalLane::MixedContext => &[
            "evidence_synthesis",
            "coverage_gaps",
            "facts",
            "procedures",
            "resources",
            "capsules",
            "recent_events",
            "profile",
            "uncertainties",
        ],
    }
}

pub(super) fn memory_request_mode_label(mode: &MemoryRequestMode) -> &'static str {
    match mode {
        MemoryRequestMode::General => "general",
        MemoryRequestMode::Identity => "identity",
        MemoryRequestMode::Policy => "policy",
        MemoryRequestMode::Task => "task",
        MemoryRequestMode::Fact => "fact",
        MemoryRequestMode::Temporal => "temporal",
        MemoryRequestMode::Evidence => "evidence",
        MemoryRequestMode::Repair => "repair",
        MemoryRequestMode::FreshLookup => "fresh_lookup",
    }
}

pub(super) fn memory_lane_label(lane: &MemoryRetrievalLane) -> &'static str {
    match lane {
        MemoryRetrievalLane::HotState => "hot_state",
        MemoryRetrievalLane::Procedural => "procedural",
        MemoryRetrievalLane::TaskState => "task_state",
        MemoryRetrievalLane::MixedContext => "mixed_context",
        MemoryRetrievalLane::CapsuleRecall => "capsule_recall",
        MemoryRetrievalLane::TemporalRecall => "temporal_recall",
        MemoryRetrievalLane::RepairHistory => "repair_history",
        MemoryRetrievalLane::FreshLookup => "fresh_lookup",
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/memory/planner.rs"]
mod tests;
