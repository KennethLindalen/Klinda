using KlindaBase.Paging.PagedCore;

namespace KlindaBase.Services.PagedServices;

public class PagedSearchService
{
    /// <summary>
    /// Finds the parent of a given child node in the tree.
    /// </summary>
    /// <param name="rootPageId">The page ID of the root of the tree.</param>
    /// <param name="childPageId">The page ID of the child node to find the parent of.</param>
    /// <param name="loadNode">A function that takes a page ID and returns the node at that page ID.</param>
    /// <returns>The parent node of the given child, or null if the child is not found in the tree.</returns>
    public static PagedInternalNode FindParent(
        int rootPageId,
        int childPageId,
        Func<int, PagedBPlusNode> loadNode)
    {
        // Start at the root of the tree
        var current = loadNode(rootPageId);

        // If the root is not an internal node, we can't find the parent
        if (current is not PagedInternalNode internalNode)
            return null;

        // Iterate over the children of the current node
        foreach (var childId in internalNode.ChildrenPageIds)
        {
            // If the child is the node we're looking for, return the parent
            if (childId == childPageId)
                return internalNode;

            // Recursively traverse the tree to find the parent
            var child = loadNode(childId);
            if (child is PagedInternalNode)
            {
                var found = FindParent(childId, childPageId, loadNode);
                if (found != null)
                    return found;
            }
        }

        // If we reach this point, the node is not in the tree
        return null;
    }
}