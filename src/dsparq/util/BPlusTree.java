package dsparq.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implementation of B+ tree
 * @author Raghava
 *
 */
public class BPlusTree<E> {

	private Node root;
	private int order;
	private Comparator<E> comparator;
	int height;
	
	public BPlusTree(int order, Comparator<E> comparator) {
		this.order = order;
		root = null;
		this.comparator = comparator;
		height = -1;
	}
	
	private class Node {
		private List<E> keys;
		private List<Node> childPointers;
		private boolean isLeaf;
		private Node siblingNode;
		private Node() {
			keys = new ArrayList<E>();
			childPointers = new ArrayList<Node>();
			isLeaf = true;
			siblingNode = null;
		}
	}
	
	public void insert(E data) {
		if(root == null) {
			root = new Node();
			root.keys.add(data);
			root.childPointers = null;
			return;
		}

		if(root.isLeaf) {
			int numKeys = root.keys.size();
			if(numKeys < (order-1)) {
				// insert in correct position
				insertInOrder(root.keys, data);
				root.childPointers = null;
				return;
			}
			else {
				//root node is full. split the root node and make a new root node
				Node newNode = new Node();
				root.siblingNode = newNode;
				root.isLeaf = true;
				newNode.isLeaf = true;
				
				insertInOrder(root.keys, data);				
				int splitIndex = (int)((numKeys/2.0) + 0.5);
				// a node was added since numKeys was assigned a value
				newNode.keys.addAll(root.keys.subList(splitIndex, numKeys+1));
				root.keys = root.keys.subList(0, splitIndex);
				
				Node newRootNode = new Node();
				newRootNode.isLeaf = false;
				newRootNode.keys.add(newNode.keys.get(0));
				newRootNode.childPointers.add(root);
				newRootNode.childPointers.add(newNode);
				root = newRootNode;
				return;
			}
		}
		else {
			// traverse to the correct node and use stack to store parent nodes
			Stack<Node> internalNodeStack = new Stack<Node>();
			// start at root
			Node node = findNode(data, root, internalNodeStack);
			insertInOrder(node.keys, data);
			if(node.keys.size() > (order-1)) {
				// node is full, split it
				splitNode(node, internalNodeStack);
			}
		}
	}
	
	private void splitNode(Node nodeToSplit, Stack<Node> stack) {
		Node newNode = new Node();
		newNode.isLeaf = nodeToSplit.isLeaf;
		int splitIndex = (int)((nodeToSplit.keys.size()/2.0) + 0.5);
		newNode.keys.addAll(nodeToSplit.keys.subList(splitIndex, 
							nodeToSplit.keys.size()));
		nodeToSplit.keys = nodeToSplit.keys.subList(0, splitIndex);
		E elementToMoveUp = newNode.keys.get(0);
		if(nodeToSplit.isLeaf) {
			// no children of nodeToSplit & newNode, since they are leaves
			newNode.siblingNode = nodeToSplit.siblingNode;
			nodeToSplit.siblingNode = newNode;
		}
		else {
			// need not have duplicate element in leaf after split
			newNode.keys.remove(0);
			// handle child pointers of nodeToSplit & newNode
			// no. of childPointers = no. of keys + 1
			int numChildPtrs = newNode.keys.size() + 1;
			int childPtrStartIndex = nodeToSplit.childPointers.size() - numChildPtrs;
			newNode.childPointers.addAll(nodeToSplit.childPointers.subList(
					childPtrStartIndex, nodeToSplit.childPointers.size()));
			nodeToSplit.childPointers = nodeToSplit.childPointers.subList(0, childPtrStartIndex);
		}
		if(stack.isEmpty()) {
			Node newRootNode = new Node();
			newRootNode.keys.add(elementToMoveUp);
			newRootNode.childPointers.add(nodeToSplit);
			newRootNode.childPointers.add(newNode);
			newRootNode.isLeaf = false;
			newRootNode.siblingNode = null;
			root = newRootNode;
			return;
		}
		Node parentNode = stack.pop();
		int insertionIndex = insertInOrder(parentNode.keys, elementToMoveUp);
		if((parentNode.keys.size()-1) <= insertionIndex) {
			// insert the child pointer at the end
			parentNode.childPointers.add(newNode);
		}
		else {
			// move the remaining pointers to the right - moves automatically
			parentNode.childPointers.add(insertionIndex+1, newNode);
		}
		if(parentNode.keys.size() > (order-1))
				splitNode(parentNode, stack);
	}
	
	private Node findNode(E data, Node startNode, Stack<Node> stack) {
		if(startNode.isLeaf)
			return startNode;
		if(comparator.compare(data, startNode.keys.get(0)) < 0) {
			stack.push(startNode);
			return findNode(data, startNode.childPointers.get(0), stack);
		}
		else if(comparator.compare(data, startNode.keys.get(startNode.keys.size()-1)) >= 0) {
			stack.push(startNode);
			return findNode(data, startNode.childPointers.get(
					startNode.childPointers.size()-1), stack);
		}
		else {
			// search within this node
			for(int i=0; i<startNode.keys.size(); i++) {
				E key = startNode.keys.get(i);
				if(comparator.compare(data, key) < 0) {
					stack.push(startNode);
					return findNode(data, startNode.childPointers.get(i), stack);
				}
				else if(comparator.compare(data, key) == 0) {
					stack.push(startNode);
					return findNode(data, startNode.childPointers.get(i+1), stack);
				}
			}
		}
		return null;
	}
	
	private int insertInOrder(List<E> keys, E data) {
		int i=0;
		int numKeys = keys.size();
		if(comparator.compare(data, keys.get(numKeys-1)) > 0) {
			keys.add(data);
			return numKeys;
		}
		while((i<numKeys) && 
				(comparator.compare(data, keys.get(i)) > 0))
			i++;
		
		// move every element to the right - taken care by List add()
		keys.add(i, data);
		return i;
	}
	
	public void search(E data) {
		throw new UnsupportedOperationException("Not implemented yet");
	}
	
	public boolean isPresent(E data) {
		Node node = root;
		while(!node.isLeaf) {
			if(comparator.compare(data, node.keys.get(node.keys.size()-1)) > 0) {
				node = node.childPointers.get(node.childPointers.size()-1);
				continue;
			}
			for(int i=0; i<node.keys.size(); i++) {
				E key = node.keys.get(i);
				int compareValue = comparator.compare(data, key);
				if(compareValue < 0) {
					node = node.childPointers.get(i);
					break;
				}
				else if(compareValue == 0) 
					return true;
			}
		}
		// check in the leaf
		if(comparator.compare(data, node.keys.get(node.keys.size()-1)) > 0)
			return false;
		int index = Collections.binarySearch(node.keys, data, comparator);
		return (index>=0) ? true : false;
/*		
		for(int i=0; i<node.keys.size(); i++) {
			E key = node.keys.get(i);
			int compareValue = comparator.compare(data, key);
			if(compareValue < 0) {
				return false;
			}
			else if(compareValue == 0) 
				return true;
		}
*/		
	}
	
	public Node getRoot() {
		return root;
	}
	
	public int getHeight() {
		Node node = root;
		height = 0;
		while(!node.isLeaf) {
			node = node.childPointers.get(0);
			height++;
		}
		return height;
	}
	
	public int getOrder() {
		return order;
	}
	
	public List<E> getAllKeys() {
		List<E> leaves = new ArrayList<E>();
		Node node = root;
		while(!node.isLeaf)
			node = node.childPointers.get(0);
		while(node != null) {
			leaves.addAll(node.keys);
			node = node.siblingNode;
		}
		return leaves;
	}
	
	public void printTree() {
		LinkedBlockingQueue<Node> queue = new LinkedBlockingQueue<Node>();
		queue.offer(root);
		while(!queue.isEmpty()) {
			Node node = queue.poll();
			for(E key : node.keys) 
				System.out.print(key + "  ");
			System.out.println();
			if(node.childPointers != null && !node.childPointers.isEmpty()) {
			for(Node childNode : node.childPointers) {
				// print childNode keys 
				for(E data : childNode.keys)
					System.out.print(data + "  ");
				System.out.print(" ; ");
				// add children of childNode to queue
				if(childNode.childPointers != null && !childNode.childPointers.isEmpty()) {
					for(Node grandChildNode : childNode.childPointers)
						queue.offer(grandChildNode);
				}
			}
			System.out.println("\n");
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		BPlusTree<Long> bplusTree = new BPlusTree<Long>(101, new LongComparator());
		Random r = new Random();
		System.out.println("Creating B+ tree of order " + bplusTree.getOrder());
		List<Long> keylst = new ArrayList<Long>();
		GregorianCalendar start = new GregorianCalendar();
		for(int i=1; i<=2000000; i++) {
			Long randomLong = new Long(Math.abs(r.nextLong()));
			bplusTree.insert(randomLong);
			keylst.add(randomLong);
		}
		Util.getElapsedTime(start);
		System.out.println("Tree height: " + bplusTree.getHeight());
		start = new GregorianCalendar();
		System.out.println("Fetching all leaves....");
		List<Long> allKeys = bplusTree.getAllKeys();
		System.out.println("Key count: " + allKeys.size());
		Util.getElapsedTime(start);
		allKeys.clear();
		System.out.println("\nSearching for all the keys...");
		start = new GregorianCalendar();
		boolean isPresent = false;
		for(Long key : keylst) {
			isPresent = bplusTree.isPresent(key);
			if(!isPresent)
				throw new Exception("Not expecting false value: " + key);
		}
		Util.getElapsedTime(start);
	
		// example-2
/*		
		BPlusTree<Long> bplusTree = new BPlusTree<Long>(4, new LongComparator());
		bplusTree.insert(new Long(2466763445449436291));
		bplusTree.insert(new Long(8931028068901162135));
		bplusTree.insert(new Long(1902906240525059880));	
		bplusTree.insert(new Long(4667878109698511170));
		bplusTree.insert(new Long(3779639213161967242));
		bplusTree.insert(new Long(6837118520223556983));
		bplusTree.insert(new Long(5505880341795738517));
		bplusTree.insert(new Long(7798426218791606090));
		System.out.println("Tree created");
		List<Long> keylst = new ArrayList<Long>();
		
		boolean isPresent = false;
		for(Long key : keylst) {
			isPresent = bplusTree.isPresent(key);
			System.out.println("Key: " + key + "  isPresent: " + isPresent);
		}
*/		
	}
}

class LongComparator implements Comparator<Long> {

	@Override
	public int compare(Long o1, Long o2) {
		long diff = o1.longValue()-o2.longValue();
		if(diff == 0)
			return 0;
		else if(diff < 0)
			return -1;
		else
			return 1;
	}	
}
