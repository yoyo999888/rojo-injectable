//! 将 Rojo project tree 合并到已有的 WeakDom（.rbxl / .rbxm）中。
//!
//! 合并策略：按 name + className 递归匹配。
//! - 匹配到 → 覆盖属性，递归子节点
//! - 未匹配到 → clone 整个 Rojo 子树插入 base
//! - base 中 Rojo 未涉及的节点 → 完全保留
//!
//! 该合并是幂等的：对同一个 base 多次执行相同 merge，
//! 输出的二进制内容完全一致。

use std::{
    collections::HashMap,
    io::BufReader,
    path::Path,
};

use anyhow::{bail, Context};
use fs_err::File;
use rbx_dom_weak::{types::Ref, Instance, Ustr, WeakDom};

/// 读取 .rbxl / .rbxlx / .rbxm / .rbxmx 文件为 WeakDom。
pub fn read_place_file(path: &Path) -> anyhow::Result<WeakDom> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .context("文件必须有 .rbxl / .rbxlx / .rbxm / .rbxmx 扩展名")?;

    let reader = BufReader::new(
        File::open(path)
            .with_context(|| format!("无法打开文件: {}", path.display()))?,
    );

    match extension {
        "rbxl" | "rbxm" => rbx_binary::from_reader(reader).with_context(|| {
            format!("无法反序列化二进制文件: {}", path.display())
        }),
        "rbxlx" | "rbxmx" => {
            let config = rbx_xml::DecodeOptions::new()
                .property_behavior(rbx_xml::DecodePropertyBehavior::ReadUnknown);
            rbx_xml::from_reader(reader, config).with_context(|| {
                format!("无法反序列化 XML 文件: {}", path.display())
            })
        }
        _ => bail!(
            "不支持的文件扩展名 .{}，期望 .rbxl / .rbxlx / .rbxm / .rbxmx",
            extension
        ),
    }
}

/// 将 Rojo project tree 合并到 base WeakDom 中。
///
/// - 如果 rojo root 是 DataModel → 递归合并 children
/// - 否则 → 整个 rojo tree clone 到 base root 下
pub fn merge_into(mut base: WeakDom, rojo: &WeakDom) -> anyhow::Result<WeakDom> {
    let rojo_root = rojo.root();
    let base_root_ref = base.root_ref();

    if rojo_root.class.as_str() != "DataModel" {
        // 非 place 项目（model）：整体 clone
        let cloned_ref = rojo.clone_into_external(rojo_root.referent(), &mut base);
        base.transfer_within(cloned_ref, base_root_ref);
        return Ok(base);
    }

    // DataModel：递归合并 children
    merge_children(&mut base, base_root_ref, rojo, rojo.root_ref());

    Ok(base)
}

/// 递归合并 rojo parent 的 children 到 base parent 下。
///
/// 对每个 rojo child：
/// - 在 base parent 的 children 中按 (name, className) 查找
/// - 找到 → 覆盖属性，递归
/// - 未找到 → clone 整个子树插入
fn merge_children(
    base: &mut WeakDom,
    base_parent_ref: Ref,
    rojo: &WeakDom,
    rojo_parent_ref: Ref,
) {
    // 先收集 rojo children 信息，避免借用冲突
    let rojo_children: Vec<(Ref, String, Ustr)> = rojo
        .get_by_ref(rojo_parent_ref)
        .unwrap()
        .children()
        .iter()
        .map(|&child_ref| {
            let child = rojo.get_by_ref(child_ref).unwrap();
            (child_ref, child.name.clone(), child.class)
        })
        .collect();

    // 为 base parent 的 children 建立 (name, class) → Ref 的 HashMap（owned keys，避免借用冲突）
    let base_child_map = build_child_map(base, base_parent_ref);

    for (rojo_child_ref, name, class) in &rojo_children {
        let key = (name.clone(), class.to_string());

        if let Some(&base_child_ref) = base_child_map.get(&key) {
            // 匹配到 → 覆盖属性，递归
            let rojo_child = rojo.get_by_ref(*rojo_child_ref).unwrap();
            update_properties(base, base_child_ref, rojo_child);
            merge_children(base, base_child_ref, rojo, *rojo_child_ref);
        } else {
            // 未匹配到 → clone 整个子树
            let cloned_ref = rojo.clone_into_external(*rojo_child_ref, base);
            base.transfer_within(cloned_ref, base_parent_ref);
        }
    }
}

/// 为 parent 的 children 建立 (name, className) → Ref 的 HashMap。
/// 使用 owned String 作为 key，避免与 base 的可变借用冲突。
/// 如果有重名（同 name + class），只保留第一个。
fn build_child_map(dom: &WeakDom, parent_ref: Ref) -> HashMap<(String, String), Ref> {
    let parent = dom.get_by_ref(parent_ref).unwrap();
    let mut map = HashMap::with_capacity(parent.children().len());

    for &child_ref in parent.children() {
        let child = dom.get_by_ref(child_ref).unwrap();
        let key = (child.name.clone(), child.class.to_string());
        map.entry(key).or_insert(child_ref);
    }

    map
}

/// 将 rojo instance 的属性覆盖到 base instance 上。
/// base 独有的属性保留不动。
fn update_properties(base: &mut WeakDom, base_ref: Ref, rojo_instance: &Instance) {
    let base_instance = base.get_by_ref_mut(base_ref).unwrap();

    // 名称同步
    base_instance.name.clone_from(&rojo_instance.name);

    // 属性覆盖（rojo 有的覆盖，rojo 没有的保留 base 原值）
    for (prop_name, prop_value) in &rojo_instance.properties {
        base_instance
            .properties
            .insert(prop_name.clone(), prop_value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbx_dom_weak::{ustr, InstanceBuilder};

    /// 辅助：创建简单的 WeakDom
    fn make_datamodel(children: Vec<InstanceBuilder>) -> WeakDom {
        let mut builder = InstanceBuilder::new("DataModel");
        for child in children {
            builder = builder.with_child(child);
        }
        WeakDom::new(builder)
    }

    fn get_child_by_name<'a>(dom: &'a WeakDom, parent_ref: Ref, name: &str) -> Option<&'a Instance> {
        let parent = dom.get_by_ref(parent_ref)?;
        for &child_ref in parent.children() {
            let child = dom.get_by_ref(child_ref)?;
            if child.name == name {
                return Some(child);
            }
        }
        None
    }

    fn count_children_by_name(dom: &WeakDom, parent_ref: Ref, name: &str) -> usize {
        let parent = dom.get_by_ref(parent_ref).unwrap();
        parent
            .children()
            .iter()
            .filter(|&&child_ref| {
                dom.get_by_ref(child_ref).unwrap().name == name
            })
            .count()
    }

    // ===== build_child_map =====

    #[test]
    fn child_map_exact_match() {
        let dom = make_datamodel(vec![
            InstanceBuilder::new("Folder").with_name("Scripts"),
            InstanceBuilder::new("Part").with_name("Block"),
        ]);
        let map = build_child_map(&dom, dom.root_ref());
        assert!(map.contains_key(&("Scripts".into(), "Folder".into())));
        assert!(map.contains_key(&("Block".into(), "Part".into())));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn child_map_same_name_different_class() {
        let dom = make_datamodel(vec![
            InstanceBuilder::new("Part").with_name("Foo"),
            InstanceBuilder::new("Folder").with_name("Foo"),
        ]);
        let map = build_child_map(&dom, dom.root_ref());
        assert!(map.contains_key(&("Foo".into(), "Part".into())));
        assert!(map.contains_key(&("Foo".into(), "Folder".into())));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn child_map_no_children() {
        let dom = make_datamodel(vec![]);
        let map = build_child_map(&dom, dom.root_ref());
        assert!(map.is_empty());
    }

    // ===== update_properties =====

    #[test]
    fn update_overwrites_existing_property() {
        use rbx_dom_weak::types::Variant;

        let mut base = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_property("Gravity", Variant::Float32(196.2)),
        ]);
        let rojo_instance = InstanceBuilder::new("Workspace")
            .with_name("Workspace")
            .with_property("Gravity", Variant::Float32(50.0));
        let rojo_dom = make_datamodel(vec![rojo_instance]);

        let base_ws_ref = base.root().children()[0];
        let rojo_ws = rojo_dom.get_by_ref(rojo_dom.root().children()[0]).unwrap();

        update_properties(&mut base, base_ws_ref, rojo_ws);

        let base_ws = base.get_by_ref(base_ws_ref).unwrap();
        assert_eq!(
            base_ws.properties.get(&ustr("Gravity")),
            Some(&Variant::Float32(50.0))
        );
    }

    #[test]
    fn update_preserves_base_only_property() {
        use rbx_dom_weak::types::Variant;

        let mut base = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_property("Gravity", Variant::Float32(196.2))
                .with_property("FallenPartsDestroyHeight", Variant::Float32(-500.0)),
        ]);
        let rojo_instance = InstanceBuilder::new("Workspace")
            .with_name("Workspace")
            .with_property("Gravity", Variant::Float32(50.0));
        let rojo_dom = make_datamodel(vec![rojo_instance]);

        let base_ws_ref = base.root().children()[0];
        let rojo_ws = rojo_dom.get_by_ref(rojo_dom.root().children()[0]).unwrap();

        update_properties(&mut base, base_ws_ref, rojo_ws);

        let base_ws = base.get_by_ref(base_ws_ref).unwrap();
        assert_eq!(
            base_ws.properties.get(&ustr("Gravity")),
            Some(&Variant::Float32(50.0))
        );
        assert_eq!(
            base_ws.properties.get(&ustr("FallenPartsDestroyHeight")),
            Some(&Variant::Float32(-500.0))
        );
    }

    #[test]
    fn update_adds_new_property() {
        use rbx_dom_weak::types::Variant;

        let mut base = make_datamodel(vec![
            InstanceBuilder::new("Workspace").with_name("Workspace"),
        ]);
        let rojo_instance = InstanceBuilder::new("Workspace")
            .with_name("Workspace")
            .with_property("Gravity", Variant::Float32(50.0));
        let rojo_dom = make_datamodel(vec![rojo_instance]);

        let base_ws_ref = base.root().children()[0];
        let rojo_ws = rojo_dom.get_by_ref(rojo_dom.root().children()[0]).unwrap();

        update_properties(&mut base, base_ws_ref, rojo_ws);

        let base_ws = base.get_by_ref(base_ws_ref).unwrap();
        assert_eq!(
            base_ws.properties.get(&ustr("Gravity")),
            Some(&Variant::Float32(50.0))
        );
    }

    // ===== merge_into =====

    #[test]
    fn merge_creates_new_instance() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace").with_name("Workspace"),
        ]);
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(InstanceBuilder::new("Folder").with_name("Scripts")),
        ]);

        let merged = merge_into(base, &rojo).unwrap();

        let _ws = get_child_by_name(&merged, merged.root_ref(), "Workspace").unwrap();
        let ws_ref = merged.root().children()[0];
        let scripts = get_child_by_name(&merged, ws_ref, "Scripts");
        assert!(scripts.is_some());
        assert_eq!(scripts.unwrap().class, "Folder");
    }

    #[test]
    fn merge_no_duplicate_on_second_merge() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace").with_name("Workspace"),
        ]);
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(InstanceBuilder::new("Folder").with_name("Scripts")),
        ]);

        // 第一次 merge
        let merged1 = merge_into(base, &rojo).unwrap();
        // 第二次 merge
        let merged2 = merge_into(merged1, &rojo).unwrap();

        let ws_ref = merged2.root().children()[0];
        assert_eq!(count_children_by_name(&merged2, ws_ref, "Scripts"), 1);
    }

    #[test]
    fn merge_preserves_base_only_children() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(InstanceBuilder::new("Part").with_name("Part1"))
                .with_child(InstanceBuilder::new("Part").with_name("Part2")),
        ]);
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(InstanceBuilder::new("Folder").with_name("Scripts")),
        ]);

        let merged = merge_into(base, &rojo).unwrap();

        let ws_ref = merged.root().children()[0];
        assert!(get_child_by_name(&merged, ws_ref, "Part1").is_some());
        assert!(get_child_by_name(&merged, ws_ref, "Part2").is_some());
        assert!(get_child_by_name(&merged, ws_ref, "Scripts").is_some());
    }

    #[test]
    fn merge_matches_by_name_and_class_not_name_alone() {
        use rbx_dom_weak::types::Variant;

        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(
                    InstanceBuilder::new("Part")
                        .with_name("Foo")
                        .with_property("Size", Variant::String("original".into())),
                )
                .with_child(InstanceBuilder::new("Folder").with_name("Foo")),
        ]);
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(
                    InstanceBuilder::new("Folder")
                        .with_name("Foo")
                        .with_property("Tag", Variant::String("injected".into())),
                ),
        ]);

        let merged = merge_into(base, &rojo).unwrap();
        let ws_ref = merged.root().children()[0];

        // Part "Foo" 不受影响
        let ws = merged.get_by_ref(ws_ref).unwrap();
        let mut part_found = false;
        let mut folder_found = false;
        for &child_ref in ws.children() {
            let child = merged.get_by_ref(child_ref).unwrap();
            if child.name == "Foo" && child.class == "Part" {
                assert_eq!(
                    child.properties.get(&ustr("Size")),
                    Some(&Variant::String("original".into()))
                );
                part_found = true;
            }
            if child.name == "Foo" && child.class == "Folder" {
                assert_eq!(
                    child.properties.get(&ustr("Tag")),
                    Some(&Variant::String("injected".into()))
                );
                folder_found = true;
            }
        }
        assert!(part_found, "Part 'Foo' should be preserved");
        assert!(folder_found, "Folder 'Foo' should be updated");
    }

    #[test]
    fn merge_deep_nested() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace").with_name("Workspace"),
        ]);
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(
                    InstanceBuilder::new("Folder")
                        .with_name("Level1")
                        .with_child(
                            InstanceBuilder::new("Folder")
                                .with_name("Level2")
                                .with_child(
                                    InstanceBuilder::new("Script").with_name("DeepScript"),
                                ),
                        ),
                ),
        ]);

        let merged = merge_into(base, &rojo).unwrap();

        let ws_ref = merged.root().children()[0];
        let _l1 = get_child_by_name(&merged, ws_ref, "Level1").unwrap();
        let l1_ref = merged.get_by_ref(ws_ref).unwrap().children()[0];
        let l2 = get_child_by_name(&merged, l1_ref, "Level2");
        assert!(l2.is_some());
        let l2_ref = merged.get_by_ref(l1_ref).unwrap().children()[0];
        let script = get_child_by_name(&merged, l2_ref, "DeepScript");
        assert!(script.is_some());
        assert_eq!(script.unwrap().class, "Script");
    }

    #[test]
    fn merge_non_datamodel_clones_entire_tree() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace").with_name("Workspace"),
        ]);
        // rojo root 不是 DataModel
        let rojo = WeakDom::new(
            InstanceBuilder::new("Model")
                .with_name("MyModel")
                .with_child(InstanceBuilder::new("Part").with_name("SubPart")),
        );

        let merged = merge_into(base, &rojo).unwrap();

        // MyModel 应该作为 root 的 child 被 clone 进去
        let model = get_child_by_name(&merged, merged.root_ref(), "MyModel");
        assert!(model.is_some());
        assert_eq!(model.unwrap().class, "Model");
    }

    #[test]
    fn merge_empty_rojo_no_change() {
        let base = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_child(InstanceBuilder::new("Part").with_name("Part1")),
        ]);
        let rojo = make_datamodel(vec![]); // 空的 DataModel

        let merged = merge_into(base, &rojo).unwrap();

        let ws = get_child_by_name(&merged, merged.root_ref(), "Workspace");
        assert!(ws.is_some());
    }

    #[test]
    fn merge_idempotent_in_memory() {
        use rbx_dom_weak::types::Variant;

        let make_base = || {
            make_datamodel(vec![
                InstanceBuilder::new("Workspace")
                    .with_name("Workspace")
                    .with_child(InstanceBuilder::new("Part").with_name("Existing")),
            ])
        };
        let rojo = make_datamodel(vec![
            InstanceBuilder::new("Workspace")
                .with_name("Workspace")
                .with_property("Gravity", Variant::Float32(50.0))
                .with_child(InstanceBuilder::new("Folder").with_name("Scripts")),
        ]);

        // merge 一次
        let merged1 = merge_into(make_base(), &rojo).unwrap();
        // 序列化
        let bytes1 = serialize_dom(&merged1);

        // 反序列化再 merge
        let base2 = rbx_binary::from_reader(bytes1.as_slice()).unwrap();
        let merged2 = merge_into(base2, &rojo).unwrap();
        let bytes2 = serialize_dom(&merged2);

        // 再来一次
        let base3 = rbx_binary::from_reader(bytes2.as_slice()).unwrap();
        let merged3 = merge_into(base3, &rojo).unwrap();
        let bytes3 = serialize_dom(&merged3);

        assert_eq!(bytes2, bytes3, "第 2 次和第 3 次 merge 后二进制应完全一致");
    }

    /// 辅助：序列化 WeakDom 为 rbxl 二进制
    fn serialize_dom(dom: &WeakDom) -> Vec<u8> {
        let root = dom.root();
        let ids = if root.class == "DataModel" {
            root.children().to_vec()
        } else {
            vec![root.referent()]
        };
        let mut buf = Vec::new();
        rbx_binary::to_writer(&mut buf, dom, &ids).unwrap();
        buf
    }
}
