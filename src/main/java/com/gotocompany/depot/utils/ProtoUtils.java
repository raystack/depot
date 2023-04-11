package com.gotocompany.depot.utils;

import com.google.protobuf.Message;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class ProtoUtils {
    public static boolean hasUnknownField(Message root) {
        List<Message> messageFields = collectNestedFields(root);
        List<Message> messageWithUnknownFields = getMessageWithUnknownFields(messageFields);
        return messageWithUnknownFields.size() > 0;
    }

    private static List<Message> collectNestedFields(Message node) {
        List<Message> output = new LinkedList<>();
        Queue<Message> stack = Collections.asLifoQueue(new LinkedList<>());
        stack.add(node);
        while (true) {
            Message current = stack.poll();
            if (current == null) {
                break;
            }
            List<Message> nestedChildNodes = current.getAllFields().values().stream()
                    .filter(field -> field instanceof Message)
                    .map(field -> (Message) field)
                    .collect(Collectors.toList());
            stack.addAll(nestedChildNodes);

            output.add(current);
        }

        return output;
    }

    private static List<Message> getMessageWithUnknownFields(List<Message> messages) {
        return messages.stream().filter(message -> message.getUnknownFields().asMap().size() > 0).collect(Collectors.toList());
    }
}
