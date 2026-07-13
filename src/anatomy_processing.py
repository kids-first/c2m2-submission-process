import itertools
import logging
import networkx as nx
import obonet
import os
import pronto
from pathlib import Path
import pandas as pd
import sys
from typing import Iterable, Optional, Dict
import warnings
from utils import load_env, read_tsv, setup_logging

class Temp_Rel:
    """
    A placeholder relationship object for when no relationship found.
    """
    def __init__(self, id, name):
        self.id = id
        self.name = name

def safe_lookup(r: Iterable[str],
                g: pronto.Ontology) -> Iterable[pronto.Relationship]:
    """
    Allows for looking up relationships in a Pronto Ontology graph without
    raising an error when the relationship does not exist.
    """
    rels = {x for x in g.relationships() if x.name == r or x.id == r}
    if len(rels) >= 1:
        return rels
    else:
        logging.warning(f"No matches for {r} in {g.metadata.ontology}")
        placeholder = Temp_Rel(id=r, name=r)
        return {placeholder}

def mix(rels: Iterable[str]) -> Iterable[str]:
    """
    Create alternatives for a relationship name to account for inconsistencies
    across graphs, including versions with spaces or underscores.
    """
    mixes = {val for x in rels
        for val in (
            x,
            x.replace(" ", "_"),
            x.replace("_", " ")
        )}
    return(mixes)

def match(rels: Iterable[str],
          graphs: Iterable[pronto.Ontology]) -> Iterable[str]:
    """
    Finds all matches for the supplied relationship names or IDs
    in the provided Pronto Ontology graphs.
    """
    matches = {z for (r, g) in itertools.product(rels, graphs)
                for y in safe_lookup(r, g) for z in (y.id, y.name)}
    return(matches)

def get_rel_ids_names(rels: Iterable[str],
                      graphs: Iterable[pronto.Ontology]) -> Iterable[str]:
    return mix(match(mix(rels), graphs))

def find_rel_subproperties(
    graphs: Iterable[pronto.Ontology],
    allowed: Iterable[str] = {"is_a", "part_of"}
) -> Iterable[str]:
    """
    Calculates subproperties of a set of relationships from a set of Pronto Ontology graphs.
    """
    allowed = get_rel_ids_names(allowed, graphs)
    allowed_rels = [allowed]
    for rel, graph in itertools.product(allowed, graphs):
        try:
            allowed_rels.append(
                set([val 
                    for x in graph.get_relationship(rel).subproperties(distance=None, with_self=True)
                    for val in (x.id, x.name)]))
        except KeyError:
            logging.info(f"The relationship {rel} is not in {graph.metadata.ontology}.")
            continue
    allowed_set = mix(set.union(*allowed_rels))

    return allowed_set

def subgraph_keep_rels(G: nx.MultiDiGraph,
                       rel_graphs: Optional[Iterable[pronto.Ontology]],
                       rel_subproperties: bool = True,
                       allowed_rels: Iterable[str] = {"is_a", "part_of"}) -> nx.MultiDiGraph:
    """
    Creates a subgraph of a networkx MultiDiGraph that includes only the
    allowed relationships specified by the user. Optionally, the user may
    provide a set of Pronto Ontology graphs to calculate subproperties of the
    allowed relationships.
    """
    H = G.__class__()               # same type (keeps directed/multigraph)
    H.graph.update(G.graph)         # optional: copy graph attributes
    H.add_nodes_from((n, d.copy()) for n, d in G.nodes(data=True))

    # Augment allowed relationships with subproperties if requested
    if rel_subproperties:
        allowed = find_rel_subproperties(graphs = rel_graphs, allowed = allowed_rels)

    # Create the subgraph for all allowed relationships
    for u, v, k, data in G.edges(keys=True, data=True):
        if k in allowed:
            H.add_edge(u, v, key=k, **data.copy())
    return H

def subgraph_keep_terms(G, allowed=['UBERON']):
    H = G.__class__()               # same type (keeps directed/multigraph)
    H.graph.update(G.graph)         # optional: copy graph attributes
    H.add_nodes_from((n, d.copy()) for n, d in G.nodes(data=True) if any(ont in n for ont in allowed))

    # Create the subgraph for all allowed relationships
    for u, v, k, data in G.edges(keys=True, data=True):
        if any(ont in u for ont in allowed) and any(ont in v for ont in allowed):
            H.add_edge(u, v, key=k, **data.copy())
    return H

def compute_depths_from_root(graph: nx.MultiDiGraph,
                             roots: Optional[Iterable[str]] = None) -> dict:
    """
    Compute shortest-path distance from the nearest root to each node.
    If roots not provided, treat nodes with out-degree==0 (no parent edges) as roots.
    (Edges are child->parent, so roots are nodes with no parents: out_degree==0)
    """
    if roots is None:
        roots = [n for n in graph.nodes() if graph.out_degree(n) == 0]
    depths = {}
    # We want depth = min distance from any root following edges child->parent (i.e., up the graph).
    # That's just BFS from each root in the reversed graph so edges go parent->child.
    rev = graph.reverse(copy=False)
    for r in roots:
        # BFS tree distances from root r in rev (root->descendants)
        # Use single_source_shortest_path_length for efficiency
        d = nx.single_source_shortest_path_length(rev, r)
        for n, dist in d.items():
            # dist is distance root -> n in rev: equivalent to distance n -> root in original
            depths[n] = min(depths.get(n, float('inf')), dist)
    return depths

def compute_common_descendants(graph: nx.MultiDiGraph,
                               term_ids: Iterable[str],
                               rel_subgraph: Optional[nx.MultiDiGraph] = None,
                               allowed_rels: Optional[Iterable[str]] = None,
                               rel_graphs: Optional[Iterable[pronto.Ontology]] = None,
                               rel_subproperties: bool = False,
                               descendants_cache: Optional[Dict] = {},
                               tie_breaker: str = "max_depth",
                               roots: Optional[Iterable[str]] = None) -> Optional[str]:
    """
    Return a single 'last' common descendants according to tie_breaker.

    tie_breaker options:
      - "max_depth": pick LCD with largest depth from root (requires computing depths);
                     ties broken by sorted(node) (deterministic).
      - "min_sum_distance": pick node minimizing sum of shortest-path distances from inputs to that node;
                            ties broken by sorted(node).
      - "min_max_distance": pick node minimizing the maximum distance from any input (minimax).
      - "lexical": pick smallest node id (sorted).
      - "arbitrary": pick any one (deterministic via sorted order).
      - "none": return entire list of lowest common descendants.

    Returns node id or None if no common descendants.
    """
    if rel_subgraph is None and allowed_rels is not None:
        rel_subgraph = subgraph_keep_rels(graph, rel_graphs, rel_subproperties, allowed_rels)
    
    descendants_cache = descendants_cache
    for tid in term_ids:
        if tid not in descendants_cache:
            descendants_cache[tid] = set([id for id in nx.descendants(rel_subgraph, tid) if id.startswith("UBERON:")])
            descendants_cache[tid].add(tid)

    common_descendants = set.intersection(*[descendants_cache[tid] for tid in term_ids])

    descendant_crossmap = {}
    for tid in common_descendants:
        descendant_crossmap[tid] = set([id for id in nx.descendants(rel_subgraph, tid) if id.startswith("UBERON:")])

    lcas = common_descendants.difference(set.union(*[descendant_crossmap[tid] for tid in common_descendants]))

    if not lcas:
        logging.info("No common descendants between the terms provided...")
        return None
    if len(lcas) == 1:
        return next(iter(lcas))
    
    if tie_breaker == "none":
        return lcas

    nodes = list(term_ids)
    if tie_breaker == "lexical":
        return sorted(lcas)[0]
    if tie_breaker == "arbitrary":
        return sorted(lcas)[0]

    if tie_breaker == "max_depth":
        depths = compute_depths_from_root(graph, roots)
        # if depth missing, treat as -inf so it's ranked low
        best = None
        best_depth = -float('inf')
        for n in lcas:
            d = depths.get(n, -float('inf'))
            if d > best_depth or (d == best_depth and n < best):
                best_depth = d
                best = n
        return best

    # For distance-based tie-breakers, compute shortest-path distances from each query node to all LCAs.
    # Since edges point child->parent, we can do single_source_shortest_path_length from each query node and read distances.
    dist_sums = {}
    dist_maxes = {}
    for q in nodes:
        lengths = nx.single_source_shortest_path_length(graph, q)
        for a in lcas:
            d = lengths.get(a, float('inf'))
            dist_sums[a] = dist_sums.get(a, 0) + d
            dist_maxes[a] = max(dist_maxes.get(a, -float('inf')), d)

    if tie_breaker == "min_sum_distance":
        best = min(lcas, key=lambda a: (dist_sums.get(a, float('inf')), a))
        return best
    if tie_breaker == "min_max_distance":
        best = min(lcas, key=lambda a: (dist_maxes.get(a, float('inf')), a))
        return best

    # default fallback
    return sorted(lcas)[0]

def main():
    load_env()
    
    dest_dir = Path(os.environ.get("DEST_DIR"))
    log_file = Path(os.environ.get("LOG_FILE"))

    setup_logging(log_file)

    # Load ontologies for understanding edges with Pronto package
    warnings.filterwarnings("ignore", category=pronto.warnings.ProntoWarning)
    ro_graph = pronto.Ontology.from_obo_library("ro.owl")
    bspo_graph = pronto.Ontology.from_obo_library("bspo.owl")

    # Load UBERON ontology with Obonet package and Networkx
    uberon_graph = obonet.read_obo("http://purl.obolibrary.org/obo/uberon/uberon-full.obo")

    # Import necessary TSVs
    biosample_in_collection_tsv = read_tsv(Path.joinpath(dest_dir, "biosample_in_collection.tsv"))
    collection_anatomy_tsv = read_tsv(Path.joinpath(dest_dir, "collection_anatomy.tsv"))
    biosample_tsv = read_tsv(Path.joinpath(dest_dir, "biosample.tsv"))

    # Create joined table from biosamples to anatomy terms in collections
    anatomy_cross = (
        biosample_in_collection_tsv[['biosample_local_id', 'collection_local_id']].
        merge(collection_anatomy_tsv[['collection_local_id', 'anatomy']],
         on = 'collection_local_id', how = 'left').
        drop_duplicates()
    )

    # Setup for calculating descendants
    processed_terms = []
    biosample_ids = anatomy_cross['biosample_local_id'].drop_duplicates().tolist()

    # Create subgraph with only allowed terms for calculating descendants
    rel_subgraph = subgraph_keep_rels(
        uberon_graph,
        rel_graphs = [ro_graph, bspo_graph],
        rel_subproperties = True,
        allowed_rels = {"is_a", "part_of"})

    # Create a cache of descendants to speed up downstream process
    all_terms = anatomy_cross[['anatomy']].drop_duplicates()['anatomy'].tolist()
    descendants_cache = {}
    for tid in all_terms:
        if tid not in descendants_cache:
            descendants_cache[tid] = set([id for id in nx.descendants(rel_subgraph, tid) if id.startswith("UBERON:")])
            descendants_cache[tid].add(tid)

    # Function for grabbing all anatomical terms associated with a BSID in collections
    def anatomy_terms(x):
        return anatomy_cross[anatomy_cross['biosample_local_id'] == x]['anatomy'].tolist()

    # Total number of BSIDs to process for calculating progress bar
    total_bsids = len(biosample_ids)

    # Iterate over biosample_ids in anatomy collections and calculate anatomy descendents for sets
    for i, bsid in enumerate(biosample_ids):
        sys.stdout.write(f"Currently processing {biosample_ids[i]} ({i / total_bsids * 100:.2f}%)\r")
        sys.stdout.flush()
        descendants = compute_common_descendants(
            uberon_graph,
            term_ids = anatomy_terms(bsid),
            rel_subgraph = rel_subgraph,
            descendants_cache = descendants_cache,
            tie_breaker = "max_depth",
            roots = ['BFO:0000001'])
        if type(descendants) is str:
            processed_terms.append([bsid, descendants])
        else:
            logging.warning(f"No single shared descendant. Descendants function returned {descendants}.")
            continue
    mapped_terms = pd.DataFrame(processed_terms)

    # Save descendent terms from collections back into biosample file.
    my_map = mapped_terms.set_index(0)[1].to_dict()

    biosample_tsv['anatomy'] = biosample_tsv['local_id'].map(my_map)

    biosample_tsv.to_csv(Path.joinpath(dest_dir, "biosample.tsv"), sep='\t', index=False)

if __name__ == "__main__":
    main()
