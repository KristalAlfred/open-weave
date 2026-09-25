use std::collections::BTreeMap;
use std::fmt::Write;

use sha2::{Digest, Sha256};
use weave_core::DesiredHop;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesiredSnapshot {
    pub(crate) revision: String,
    pub(crate) hops: Vec<DesiredHop>,
}

pub(crate) fn snapshots(
    desired: BTreeMap<String, Vec<DesiredHop>>,
) -> BTreeMap<String, DesiredSnapshot> {
    desired
        .into_iter()
        .map(|(node_id, hops)| {
            let mut hash = Sha256::new();
            hash.update(b"open-weave desired snapshot\0");
            hash.update((node_id.len() as u64).to_be_bytes());
            hash.update(node_id.as_bytes());
            hash.update(serde_json::to_vec(&hops).expect("desired hops always serialize as JSON"));
            let digest = hash.finalize();
            let mut revision = String::with_capacity(digest.len() * 2);
            for byte in digest {
                write!(revision, "{byte:02x}").expect("writing to a string cannot fail");
            }
            (node_id, DesiredSnapshot { revision, hops })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{DesiredEgress, DeviceKind, HopRole, SocketSpec};

    fn hop(id: &str, node_id: &str, branch_id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: node_id.to_string(),
            profile_id: "test-profile".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::Device(DeviceKind::Capture),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: branch_id.to_string(),
                socket: SocketSpec::Device(DeviceKind::Display),
            }],
        }
    }

    fn revision(node_id: &str, hops: Vec<DesiredHop>) -> String {
        snapshots(BTreeMap::from([(node_id.to_string(), hops)]))[node_id]
            .revision
            .clone()
    }

    #[test]
    fn same_node_and_ordered_hops_have_a_stable_revision() {
        let hops = vec![hop("hop-a", "node-a", "studio")];

        assert_eq!(revision("node-a", hops.clone()), revision("node-a", hops));
    }

    #[test]
    fn node_or_hop_changes_change_the_revision() {
        let hops = vec![hop("hop-a", "node-a", "studio")];
        let original = revision("node-a", hops.clone());

        assert_ne!(original, revision("node-b", hops));
        assert_ne!(
            original,
            revision("node-a", vec![hop("hop-a", "node-a", "preview")])
        );
    }

    #[test]
    fn empty_desired_state_has_a_stable_revision() {
        let first = revision("node-a", Vec::new());
        let second = revision("node-a", Vec::new());

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }
}
